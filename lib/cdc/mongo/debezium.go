package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Debezium struct{}

func (Debezium) GetEventFromBytes(bytes []byte) (cdc.Event, error) {
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	var schemaEventPayload SchemaEventPayload
	if err := json.Unmarshal(bytes, &schemaEventPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	// Now marshal before & after string.
	if schemaEventPayload.Payload.Before != nil {
		before, err := typing.JSONEToMap([]byte(*schemaEventPayload.Payload.Before))
		if err != nil {
			return nil, err
		}

		schemaEventPayload.Payload.beforeMap = before
	}

	if schemaEventPayload.Payload.After != nil {
		after, err := typing.JSONEToMap([]byte(*schemaEventPayload.Payload.After))
		if err != nil {
			return nil, fmt.Errorf("failed to call mongo JSONEToMap: %w", err)
		}

		schemaEventPayload.Payload.afterMap = after
	}

	return &schemaEventPayload, nil
}

func (Debezium) Labels() []string {
	return []string{constants.DBZMongoFormat}
}

func (Debezium) GetPrimaryKey(key []byte, tc kafkalib.TopicConfig, reservedColumns map[string]bool) (map[string]any, error) {
	kvMap, err := debezium.ParsePartitionKey(key, tc.CDCKeyFormat, reservedColumns)
	if err != nil {
		return nil, err
	}

	// This code is needed because the partition key bytes returns nested objects as a string
	// Such that, the value looks like this: {"id":"{\"$oid\": \"640127e4beeb1ccfc821c25b\"}"}
	for k, v := range kvMap {
		var obj map[string]any
		if err = json.Unmarshal([]byte(fmt.Sprint(v)), &obj); err != nil {
			continue
		}

		// If the value is indeed a nested JSON object, we'll pass it along.
		kvMap[k] = obj
	}

	// Now that we have the JSON extended object, we'll parse it down into bytes, so we can feed it into `JSONEToMap`
	kvMapBytes, err := bson.MarshalExtJSON(kvMap, false, false)
	if err != nil {
		return nil, err
	}

	kvMap, err = typing.JSONEToMap(kvMapBytes)
	if err != nil {
		return nil, err
	}

	value, ok := kvMap["id"]
	if ok {
		// Debezium will write MongoDB's primary key `_id` as `id` in the partition key, so we are renaming it back to `_id`
		kvMap["_id"] = value
		delete(kvMap, "id")
	}

	return kvMap, nil
}

func (s *SchemaEventPayload) Operation() constants.Operation {
	return s.Payload.Operation
}

func (s *SchemaEventPayload) DeletePayload() bool {
	return s.Payload.Operation == constants.Delete
}

func (s *SchemaEventPayload) GetExecutionTime() time.Time {
	return time.UnixMilli(s.Payload.Source.TsMs).UTC()
}

func (s *SchemaEventPayload) GetTableName() string {
	return s.Payload.Source.Collection
}

func (s *SchemaEventPayload) GetFullTableName() string {
	// MongoDB doesn't have schemas, the full table name is the same as the table name.
	return s.GetTableName()
}

func (s *SchemaEventPayload) GetSourceMetadata() (string, error) {
	json, err := json.Marshal(s.Payload.Source)
	if err != nil {
		return "", err
	}

	return string(json), nil
}

func (s *SchemaEventPayload) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	// MongoDB does not have a schema at the database level.
	return nil, nil
}

func (s *SchemaEventPayload) GetColumns(reservedColumns map[string]bool) []columns.Column {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil
	}

	var cols []columns.Column
	for _, field := range fieldsObject.Fields {
		// We are purposefully doing this to ensure that the correct typing is set
		// When we invoke event.Save()
		cols = append(cols, columns.NewColumn(columns.EscapeName(field.FieldName, reservedColumns), typing.Invalid))
	}

	return cols
}

func (s *SchemaEventPayload) GetData(tc kafkalib.TopicConfig) (map[string]any, error) {
	var retMap map[string]any

	switch s.Operation() {
	case constants.Delete:
		// This is a delete event, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		if len(s.Payload.beforeMap) > 0 {
			retMap = s.Payload.beforeMap
		} else {
			retMap = make(map[string]any)
		}

		retMap[constants.DeleteColumnMarker] = true
		// For now, assume we only want to set the deleted column and leave other values alone.
		// If previous values for the other columns are in memory (not flushed yet), [TableData.InsertRow] will handle
		// filling them in and setting this to false.
		retMap[constants.OnlySetDeleteColumnMarker] = true
	case constants.Create, constants.Update, constants.Backfill:
		retMap = s.Payload.afterMap
		retMap[constants.DeleteColumnMarker] = false
		retMap[constants.OnlySetDeleteColumnMarker] = false
	default:
		return nil, fmt.Errorf("unknown operation: %q", s.Operation())
	}

	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = time.Now().UTC()
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = s.GetExecutionTime().UTC()
	}

	return retMap, nil
}
