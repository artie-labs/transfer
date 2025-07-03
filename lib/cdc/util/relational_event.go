package util

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/3b9989ed8c659771958fe481f248397a
type SchemaEventPayload struct {
	Schema  debezium.Schema `json:"schema"`
	Payload Payload         `json:"payload"`
}

type Payload struct {
	Before    map[string]any      `json:"before"`
	After     map[string]any      `json:"after"`
	Source    Source              `json:"source"`
	Operation constants.Operation `json:"op"`
}

type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema,omitempty"`
	Table     string `json:"table"`

	// MySQL specific
	File string  `json:"file,omitempty"`
	Pos  int64   `json:"pos,omitempty"`
	Gtid *string `json:"gtid,omitempty"`
	// MSSQL specific
	LSN           any    `json:"lsn,omitempty"`
	TransactionID *int64 `json:"transaction_id,omitempty"`
}

func shouldParseValue(value any) bool {
	if str, ok := value.(string); ok {
		// We can't parse this because this is a formula, not a value.
		if str == "CURRENT_TIMESTAMP" {
			return false
		}
	}

	return true
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
		col := columns.NewColumn(columns.EscapeName(field.FieldName), typing.Invalid)
		if shouldParseValue(field.Default) {
			val, err := field.ParseValue(field.Default)
			if err != nil {
				return nil, fmt.Errorf("failed to parse field %q for default value: %w", field.FieldName, err)
			} else {
				if field.ShouldSetDefaultValue(val) {
					col.SetDefaultValue(val)
				}
			}
		}

		cols.AddColumn(col)
	}

	return &cols, nil
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
	return s.Payload.Source.Table
}

func (s *SchemaEventPayload) GetFullTableName() string {
	if s.Payload.Source.Schema != "" {
		return s.Payload.Source.Schema + "." + s.Payload.Source.Table
	}

	return s.Payload.Source.Table
}

func (s *SchemaEventPayload) GetSourceMetadata() (string, error) {
	json, err := json.Marshal(s.Payload.Source)
	if err != nil {
		return "", err
	}

	return string(json), nil
}

func (s *SchemaEventPayload) GetData(tc kafkalib.TopicConfig) (map[string]any, error) {
	var err error
	var retMap map[string]any
	switch s.Operation() {
	case constants.Delete:
		if len(s.Payload.Before) > 0 {
			retMap, err = s.parseAndMutateMapInPlace(s.Payload.Before, debezium.Before)
			if err != nil {
				return nil, err
			}
		} else {
			retMap = make(map[string]any)
		}
		// This is a delete payload, so mark it as deleted.
		// And we need to reconstruct the data bit since it will be empty.
		// We _can_ rely on *before* since even without running replicate identity, it will still copy over
		// the PK. We can explore simplifying this interface in the future by leveraging before.
		retMap[constants.DeleteColumnMarker] = true
		// For now, assume we only want to set the deleted column and leave other values alone.
		// If previous values for the other columns are in memory (not flushed yet), [TableData.InsertRow] will handle
		// filling them in and setting this to false.
		retMap[constants.OnlySetDeleteColumnMarker] = true
	case constants.Create, constants.Update, constants.Backfill:
		retMap, err = s.parseAndMutateMapInPlace(s.Payload.After, debezium.After)
		if err != nil {
			return nil, err
		}
		retMap[constants.DeleteColumnMarker] = false
		retMap[constants.OnlySetDeleteColumnMarker] = false
	default:
		return nil, fmt.Errorf("unknown operation %q", s.Operation())
	}

	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = time.Now().UTC()
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = s.GetExecutionTime()
	}

	return retMap, nil
}

// parseAndMutateMapInPlace will take `retMap` and `kind` (which part of the schema should we be inspecting) and then parse the values accordingly.
// This will unpack any Debezium-specific values and convert them back into their original types.
// NOTE: `retMap` and the returned object are the same object.
func (s *SchemaEventPayload) parseAndMutateMapInPlace(retMap map[string]any, kind debezium.FieldLabelKind) (map[string]any, error) {
	if schemaObject := s.Schema.GetSchemaFromLabel(kind); schemaObject != nil {
		for _, field := range schemaObject.Fields {
			fieldVal, ok := retMap[field.FieldName]
			if !ok {
				continue
			}

			if val, parseErr := field.ParseValue(fieldVal); parseErr == nil {
				retMap[field.FieldName] = val
			} else {
				return nil, fmt.Errorf("failed to parse field %q: %w", field.FieldName, parseErr)
			}
		}
	}

	return retMap, nil
}
