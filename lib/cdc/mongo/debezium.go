package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/mongo"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(typingSettings typing.Settings, bytes []byte) (cdc.Event, error) {
	var schemaEventPayload SchemaEventPayload
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	err := json.Unmarshal(bytes, &schemaEventPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json, err: %v", err)
	}

	// Now marshal before & after string.
	if schemaEventPayload.Payload.Before != nil {
		before, err := mongo.JSONEToMap([]byte(*schemaEventPayload.Payload.Before))
		if err != nil {
			return nil, err
		}

		schemaEventPayload.Payload.BeforeMap = before
	}

	if schemaEventPayload.Payload.After != nil {
		after, err := mongo.JSONEToMap([]byte(*schemaEventPayload.Payload.After))
		if err != nil {
			return nil, fmt.Errorf("mongo JSONEToMap err: %v", err)
		}

		// Now, we need to iterate over each key and if the value is JSON
		// We need to parse the JSON into a string format
		for key, value := range after {
			if typing.ParseValue(typingSettings, key, nil, value) == typing.Struct {
				valBytes, err := json.Marshal(value)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal, err: %v", err)
				}

				after[key] = string(valBytes)
			}
		}

		schemaEventPayload.Payload.AfterMap = after
	}

	return &schemaEventPayload, nil
}

func (d *Debezium) Labels() []string {
	return []string{constants.DBZMongoFormat}
}

func (d *Debezium) GetPrimaryKey(key []byte, tc *kafkalib.TopicConfig) (map[string]interface{}, error) {
	kvMap, err := debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
	if err != nil {
		return nil, err
	}

	// This code is needed because the partition key bytes returns nested objects as a string
	// Such that, the value looks like this: {"id":"{\"$oid\": \"640127e4beeb1ccfc821c25b\"}"}
	for k, v := range kvMap {
		var obj map[string]interface{}
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

func (s *SchemaEventPayload) GetOptionalSchema() map[string]typing.KindDetails {
	// MongoDB does not have a schema at the database level.
	return nil
}

func (s *SchemaEventPayload) GetColumns() *columns.Columns {
	fieldsObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil
	}

	var cols columns.Columns
	for _, field := range fieldsObject.Fields {
		// We are purposefully doing this to ensure that the correct typing is set
		// When we invoke event.Save()
		cols.AddColumn(columns.NewColumn(columns.EscapeName(field.FieldName), typing.Invalid))
	}

	return &cols
}

func (s *SchemaEventPayload) GetData(pkMap map[string]interface{}, tc *kafkalib.TopicConfig) map[string]interface{} {
	var retMap map[string]interface{}
	if len(s.Payload.AfterMap) == 0 {
		// This is a delete event, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			constants.DeleteColumnMarker: true,
		}

		for k, v := range pkMap {
			retMap[k] = v
		}

		// If idempotency key is an empty string, don't put it in the event data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(ext.ISO8601)
		}
	} else {
		retMap = s.Payload.AfterMap
		// We need this because there's an edge case with Debezium
		// Where _id gets rewritten as id in the partition key.
		for k, v := range pkMap {
			retMap[k] = v
		}

		retMap[constants.DeleteColumnMarker] = false
	}

	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = ext.NewUTCTime(ext.ISO8601)
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = s.GetExecutionTime().Format(ext.ISO8601)
	}

	return retMap
}
