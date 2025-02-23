package mongo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/mongo"
	"go.mongodb.org/mongo-driver/bson"
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
		before, err := mongo.JSONEToMap([]byte(*schemaEventPayload.Payload.Before))
		if err != nil {
			return nil, err
		}

		schemaEventPayload.Payload.beforeMap = before
	}

	if schemaEventPayload.Payload.After != nil {
		after, err := mongo.JSONEToMap([]byte(*schemaEventPayload.Payload.After))
		if err != nil {
			return nil, fmt.Errorf("failed to call mongo JSONEToMap: %w", err)
		}

		// Now, let's iterate over each key. If the value is a map, we'll need to JSON marshal it.
		// We do this to ensure parity with how relational Debezium emits the message.
		for key, value := range after {
			switch value.(type) {
			case nil, string, int, int32, int64, float32, float64, bool:
				continue
			default:
				if reflect.TypeOf(value).Kind() == reflect.Map {
					valBytes, err := json.Marshal(value)
					if err != nil {
						return nil, fmt.Errorf("failed to marshal: %w", err)
					}

					after[key] = string(valBytes)
				}
			}
		}

		schemaEventPayload.Payload.afterMap = after
	}

	return &schemaEventPayload, nil
}

func (Debezium) Labels() []string {
	return []string{constants.DBZMongoFormat}
}

func (Debezium) GetPrimaryKey(key []byte, tc kafkalib.TopicConfig) (map[string]any, error) {
	kvMap, err := debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
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

	kvMap, err = mongo.JSONEToMap(kvMapBytes)
	if err != nil {
		return nil, err
	}

	value, isOk := kvMap["id"]
	if isOk {
		// Debezium will write MongoDB's primary key `_id` as `id` in the partition key, so we are renaming it back to `_id`
		kvMap["_id"] = value
		delete(kvMap, "id")
	}

	return kvMap, nil
}

func (s *SchemaEventPayload) Operation() string {
	return s.Payload.Operation
}

func (s *SchemaEventPayload) DeletePayload() bool {
	return s.Payload.Operation == "d"
}

func (s *SchemaEventPayload) GetExecutionTime() time.Time {
	return time.UnixMilli(s.Payload.Source.TsMs).UTC()
}

func (s *SchemaEventPayload) GetTableName() string {
	return s.Payload.Source.Collection
}

func (s *SchemaEventPayload) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	// MongoDB does not have a schema at the database level.
	return nil, nil
}

func (s *SchemaEventPayload) GetColumns() (*columns.Columns, error) {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil, nil
	}

	var cols columns.Columns
	for _, field := range fieldsObject.Fields {
		// We are purposefully doing this to ensure that the correct typing is set
		// When we invoke event.Save()
		cols.AddColumn(columns.NewColumn(columns.EscapeName(field.FieldName), typing.Invalid))
	}

	return &cols, nil
}

func (s *SchemaEventPayload) GetData(tc kafkalib.TopicConfig) (map[string]any, error) {
	var retMap map[string]any

	switch s.Operation() {
	case "d":
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
	case "r", "u", "c":
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
