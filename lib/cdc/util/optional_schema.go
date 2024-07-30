package util

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil, nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		kd, err := field.ToKindDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to convert field (%v), to kind details: %w", field, err)
		}

		if kd == typing.Invalid {
			slog.Warn("Skipping field from optional schema b/c we cannot determine the data type", slog.String("field", field.FieldName))
			continue
		}

		schema[field.FieldName] = kd
	}

	return schema, nil
}
