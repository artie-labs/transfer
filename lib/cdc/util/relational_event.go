package util

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
	"strconv"
	"time"
)

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/3b9989ed8c659771958fe481f248397a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload payload         `json:"payload"`
}

type payload struct {
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    Source                 `json:"source"`
	Operation string                 `json:"op"`
}

type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
}

func (s *SchemaEventPayload) GetOptionalSchema(ctx context.Context) map[string]typing.KindDetails {
	fieldsObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		// So far, we should only need to add a string type.
		// (a) All the special Debezium types will be handled by our Debezium library and casted accordingly.
		// (b) All the ZonedTimestamps where the actual casting is from a string will be handled by our typing library
		// We are explicitly adding this for string types where the value may be of time/date kind but
		// the actual column type in the source database is STRING.
		if field.Type == "string" && field.DebeziumType == "" {
			schema[field.FieldName] = typing.String
		}
	}

	return schema
}

func (s *SchemaEventPayload) GetExecutionTime() time.Time {
	return time.UnixMilli(s.Payload.Source.TsMs).UTC()
}

func (s *SchemaEventPayload) GetData(ctx context.Context, pkMap map[string]interface{}, tc *kafkalib.TopicConfig) map[string]interface{} {
	retMap := make(map[string]interface{})
	if len(s.Payload.After) == 0 {
		// This is a delete payload, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]interface{}{
			constants.DeleteColumnMarker: true,
		}

		for k, v := range pkMap {
			retMap[k] = v
		}

		// If idempotency key is an empty string, don't put it in the payload data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(time.RFC3339)
		}
	} else {
		retMap = s.Payload.After
		retMap[constants.DeleteColumnMarker] = false
	}

	// Iterate over the schema and identify if there are any fields that require extra care.
	afterSchemaObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if afterSchemaObject != nil {
		for _, field := range afterSchemaObject.Fields {
			// Check if the field is an integer and requires us to cast it as such.
			if field.IsInteger() {
				valFloat, isOk := retMap[field.FieldName].(float64)
				if isOk {
					retMap[field.FieldName] = int(valFloat)
					continue
				}
			}

			if valid, supportedType := debezium.RequiresSpecialTypeCasting(field.DebeziumType); valid {
				val, isOk := retMap[field.FieldName]
				if isOk {
					// Need to cast this as a FLOAT first because the number may come out in scientific notation
					// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
					floatVal, castErr := strconv.ParseFloat(fmt.Sprint(val), 64)
					if castErr == nil {
						extendedTime, err := debezium.FromDebeziumTypeToTime(supportedType, int64(floatVal))
						if err == nil {
							retMap[field.FieldName] = extendedTime
						} else {
							logger.FromContext(ctx).WithFields(map[string]interface{}{
								"err":           err,
								"supportedType": supportedType,
								"val":           val,
							}).Debug("skipped casting dbz type due to an error")
						}
					} else {
						logger.FromContext(ctx).WithFields(map[string]interface{}{
							"err":           castErr,
							"supportedType": supportedType,
							"val":           val,
						}).Debug("skipped casting because we failed to parse the float")
					}
				}
			}
		}
	}

	return retMap
}
