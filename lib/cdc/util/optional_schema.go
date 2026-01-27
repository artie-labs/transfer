package util

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema(cfg config.SharedDestinationSettings) (map[string]typing.KindDetails, error) {
	fieldsObject := s.Schema.GetSchemaFromLabel(debezium.After)
	if fieldsObject == nil {
		return nil, nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		kd, err := field.ToKindDetails(cfg)
		if err != nil {
			return nil, err
		}

		schema[field.FieldName] = kd
	}

	return schema, nil
}
