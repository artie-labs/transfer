package util

import (
	"slices"
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
		col.SetDefaultValue(parseField(field, field.Default))
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

func (s *SchemaEventPayload) GetData(_ map[string]any, tc *kafkalib.TopicConfig) map[string]any {
	retMap := make(map[string]any)
	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = ext.NewUTCTime(ext.ISO8601)
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = s.GetExecutionTime().Format(ext.ISO8601)
	}

	if len(s.Payload.After) == 0 {
		return s.processPayload(retMap, cdc.Before)
	}

	return s.processPayload(retMap, cdc.After)
}

func (s *SchemaEventPayload) processPayload(retMap map[string]any, kind cdc.FieldLabelKind) map[string]any {
	if !slices.Contains([]cdc.FieldLabelKind{cdc.After, cdc.Before}, kind) {
		return nil
	}

	if kind == cdc.Before {
		for key, value := range s.Payload.Before {
			retMap[key] = value
		}
		retMap[constants.DeleteColumnMarker] = true
	} else {
		for key, value := range s.Payload.After {
			retMap[key] = value
		}
		retMap[constants.DeleteColumnMarker] = false
	}

	schemaObject := s.Schema.GetSchemaFromLabel(kind)
	if schemaObject != nil {
		for _, field := range schemaObject.Fields {
			_, isOk := retMap[field.FieldName]
			if !isOk {
				// Skipping b/c envelope mismatch with the actual request body
				continue
			}

			retMap[field.FieldName] = parseField(field, retMap[field.FieldName])
		}
	}

	return retMap
}
