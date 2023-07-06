package mongo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/mongo"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(_ context.Context, bytes []byte) (cdc.Event, error) {
	var schemaEventPayload SchemaEventPayload
	if len(bytes) == 0 {
		// This is a Kafka Tombstone event.
		schemaEventPayload.Payload.Operation = "d"
		return &schemaEventPayload, nil
	}

	err := json.Unmarshal(bytes, &schemaEventPayload)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		// Now, we need to iterate over each key and if the value is JSON
		// We need to parse the JSON into a string format
		for key, value := range after {
			if typing.ParseValue(key, nil, value) == typing.Struct {
				valBytes, err := json.Marshal(value)
				if err != nil {
					return nil, err
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

func (d *Debezium) GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (kvMap map[string]interface{}, err error) {
	return debezium.ParsePartitionKey(key, tc.CDCKeyFormat)
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

func (s *SchemaEventPayload) GetOptionalSchema(ctx context.Context) map[string]typing.KindDetails {
	// MongoDB does not have a schema at the database level.
	return nil
}

func (s *SchemaEventPayload) GetColumns(ctx context.Context) *columns.Columns {
	fieldsObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil
	}

	var cols columns.Columns
	for _, field := range fieldsObject.Fields {
		// We are purposefully doing this to ensure that the correct typing is set
		// When we invoke event.Save()
		cols.AddColumn(columns.NewColumn(field.FieldName, typing.Invalid))
	}

	return &cols
}

func (s *SchemaEventPayload) GetData(ctx context.Context, pkMap map[string]interface{}, tc *kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})
	if len(s.Payload.AfterMap) == 0 {
		// This is a delete event, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			constants.DeleteColumnMarker: true,
			constants.UpdateColumnMarker: time.Now().UTC(),
		}

		for k, v := range pkMap {
			retMap[k] = v
		}

		// If idempotency key is an empty string, don't put it in the event data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(time.RFC3339)
		}
	} else {
		retMap = s.Payload.AfterMap
		// We need this because there's an edge case with Debezium
		// Where _id gets rewritten as id in the partition key.
		for k, v := range pkMap {
			retMap[k] = v
		}

		retMap[constants.DeleteColumnMarker] = false
		retMap[constants.UpdateColumnMarker] = time.Now().UTC()
	}

	return retMap
}
