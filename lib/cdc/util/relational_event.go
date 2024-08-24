package util

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
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
		val, parseErr := field.ParseValue(field.Default)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse field %q for default value: %w", field.FieldName, parseErr)
		} else {
			// Filter uuid.Nil and time.Zero
			col.SetDefaultValue(val)
		}

		cols.AddColumn(col)
	}

	return &cols, nil
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

func (s *SchemaEventPayload) GetData(pkMap map[string]any, tc kafkalib.TopicConfig) (map[string]any, error) {
	var retMap map[string]any
	if len(s.Payload.After) == 0 {
		if len(s.Payload.Before) > 0 {
			var err error
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
		for k, v := range pkMap {
			retMap[k] = v
		}
	} else {
		var err error
		retMap, err = s.parseAndMutateMapInPlace(s.Payload.After, debezium.After)
		if err != nil {
			return nil, err
		}
		retMap[constants.DeleteColumnMarker] = false
		retMap[constants.OnlySetDeleteColumnMarker] = false
	}

	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = ext.NewExtendedTime(time.Now().UTC(), ext.DateTimeKindType, ext.ISO8601)
	}

	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = ext.NewExtendedTime(s.GetExecutionTime(), ext.DateTimeKindType, ext.ISO8601)
	}

	return retMap, nil
}

// parseAndMutateMapInPlace will take `retMap` and `kind` (which part of the schema should we be inspecting) and then parse the values accordingly.
// This will unpack any Debezium-specific values and convert them back into their original types.
// NOTE: `retMap` and the returned object are the same object.
func (s *SchemaEventPayload) parseAndMutateMapInPlace(retMap map[string]any, kind debezium.FieldLabelKind) (map[string]any, error) {
	if schemaObject := s.Schema.GetSchemaFromLabel(kind); schemaObject != nil {
		for _, field := range schemaObject.Fields {
			fieldVal, isOk := retMap[field.FieldName]
			if !isOk {
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
