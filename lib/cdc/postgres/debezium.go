package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/debezium"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Debezium string

func (d *Debezium) GetEventFromBytes(ctx context.Context, bytes []byte) (cdc.Event, error) {
	var event SchemaEventPayload
	err := json.Unmarshal(bytes, &event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (d *Debezium) Label() string {
	return config.DBZPostgresFormat
}

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

func (s *SchemaEventPayload) Table() string {
	return s.Payload.Source.Table
}

func (s *SchemaEventPayload) GetData(pkName string, pkVal interface{}, tc *kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})
	if len(s.Payload.After) == 0 {
		// This is a delete payload, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			config.DeleteColumnMarker: true,
			pkName:                    pkVal,
		}

		// If idempotency key is an empty string, don't put it in the payload data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(time.RFC3339)
		}
	} else {
		retMap = s.Payload.After
		retMap[config.DeleteColumnMarker] = false
	}

	// Iterate over the schema and identify if there are any fields that require extra care.
	afterSchemaObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if afterSchemaObject != nil {
		for _, field := range afterSchemaObject.Fields {
			if valid, supportedType := debezium.RequiresSpecialTypeCasting(field.DebeziumType); valid {
				val, isOk := retMap[field.FieldName]
				if isOk {
					// Need to cast this as a FLOAT first because the number may come out in scientific notation
					// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
					floatVal, castErr := strconv.ParseFloat(fmt.Sprint(val), 64)
					if castErr == nil {
						retMap[field.FieldName] = debezium.FromDebeziumTypeToTime(supportedType, int64(floatVal)).Format(time.RFC3339)
					}
				}
			}
		}
	}

	return retMap
}
