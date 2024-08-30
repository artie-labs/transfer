package util

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema() map[string]typing.KindDetails {
	if fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After); fieldsObject != nil {
		schema := make(map[string]typing.KindDetails)
		for _, field := range fieldsObject.Fields {
			if kd := field.ToKindDetails(); kd != typing.Invalid {
				schema[field.FieldName] = kd
			}
		}

		return schema
	}

	return nil
}
