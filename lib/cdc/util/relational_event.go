package util

import (
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

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
	Before    map[string]any `json:"before"`
	After     map[string]any `json:"after"`
	Source    Source         `json:"source"`
	Operation string         `json:"op"`
}

type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
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
		col := columns.NewColumn(columns.EscapeName(field.FieldName), typing.Invalid)
		val, parseErr := field.ParseValue(field.Default)
		if parseErr != nil {
			slog.Warn("Failed to parse field, using original value", slog.Any("err", parseErr),
				slog.String("field", field.FieldName), slog.Any("value", field.Default))
			col.SetDefaultValue(field.Default)
		} else {
			col.SetDefaultValue(val)
		}

		cols.AddColumn(col)
	}

	return &cols
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
	return s.Payload.Source.Table
}

func (s *SchemaEventPayload) GetData(pkMap map[string]any, tc *kafkalib.TopicConfig) map[string]any {
	var retMap map[string]any
	if len(s.Payload.After) == 0 {
		// This is a delete payload, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap = map[string]any{
			constants.DeleteColumnMarker: true,
		}

		for k, v := range pkMap {
			retMap[k] = v
		}

		// If idempotency key is an empty string, don't put it in the payload data
		if tc.IdempotentKey != "" {
			retMap[tc.IdempotentKey] = s.GetExecutionTime().Format(ext.ISO8601)
		}
	} else {
		retMap = s.Payload.After
		retMap[constants.DeleteColumnMarker] = false
	}

	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = ext.NewUTCTime(ext.ISO8601)
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = s.GetExecutionTime().Format(ext.ISO8601)
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

			val, parseErr := field.ParseValue(retMap[field.FieldName])
			if parseErr == nil {
				retMap[field.FieldName] = val
			} else {
				slog.Warn("Failed to parse field, using original value", slog.Any("err", parseErr),
					slog.String("field", field.FieldName), slog.Any("value", retMap[field.FieldName]))
			}
		}
	}

	return retMap
}
