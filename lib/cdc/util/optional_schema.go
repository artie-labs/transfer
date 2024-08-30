package util

import (
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema() map[string]typing.KindDetails {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		return nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		if kd := field.ToKindDetails(); kd != typing.Invalid {
			schema[field.FieldName] = kd
		}
	}

	return schema
}
