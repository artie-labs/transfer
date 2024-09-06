package util

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		return nil, nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		kd, err := field.ToKindDetails()
		if err != nil {
			return nil, err
		}

		schema[field.FieldName] = kd
	}

	return schema, nil
}
