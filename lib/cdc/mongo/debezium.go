package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/mongo"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(_ context.Context, bytes []byte) (cdc.Event, error) {
	var schemaEventPayload SchemaEventPayload
	if len(bytes) == 0 {
		// This is a Kafka Tombstone event.
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
			if typing.ParseValue(value) == typing.Struct {
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

// GetPrimaryKey - Will read from the Kafka message's partition key to get the primary key for the row.
// TODO: This should support: key.converter.schemas.enable=true
func (d *Debezium) GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (pkName string, pkValue interface{}, err error) {
	switch tc.CDCKeyFormat {
	case "org.apache.kafka.connect.json.JsonConverter":
		return util.ParseJSONKey(key)
	case "org.apache.kafka.connect.storage.StringConverter":
		return util.ParseStringKey(key)
	default:
		err = fmt.Errorf("format: %s is not supported", tc.CDCKeyFormat)
	}

	return
}

func (s *SchemaEventPayload) GetExecutionTime() time.Time {
	return time.UnixMilli(s.Payload.Source.TsMs).UTC()
}

func (s *SchemaEventPayload) GetData(ctx context.Context, pkName string, pkVal interface{}, tc *kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})
	if len(s.Payload.AfterMap) == 0 {
		// This is a delete event, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			constants.DeleteColumnMarker: true,
			pkName:                       pkVal,
		}

		// If idempotency key is an empty string, don't put it in the event data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(time.RFC3339)
		}
	} else {
		retMap = s.Payload.AfterMap
		// We need this because there's an edge case with Debezium
		// Where _id gets rewritten as id in the partition key.
		retMap[pkName] = pkVal
		retMap[constants.DeleteColumnMarker] = false
	}

	return retMap
}
