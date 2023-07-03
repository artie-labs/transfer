package util

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/3b9989ed8c659771958fe481f248397a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload Payload         `json:"payload"`
}

type Payload struct {
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
		col := columns.NewColumn(field.FieldName, typing.Invalid)
		col.SetDefaultValue(parseField(ctx, field, field.Default))
		cols.AddColumn(col)
	}

	return &cols
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

func (s *SchemaEventPayload) DeletePayload() bool {
	return s.Payload.Operation == "d"
}

func (s *SchemaEventPayload) GetExecutionTime() time.Time {
	return time.UnixMilli(s.Payload.Source.TsMs).UTC()
}

func (s *SchemaEventPayload) GetTableName() string {
	return s.Payload.Source.Table
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
			_, isOk := retMap[field.FieldName]
			if !isOk {
				// Skipping b/c envelope mismatch with the actual request body
				continue
			}

			retMap[field.FieldName] = parseField(ctx, field, retMap[field.FieldName])
		}
	}

	return retMap
}
