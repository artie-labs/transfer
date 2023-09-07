package util

import (
	"context"

	"github.com/artie-labs/transfer/lib/logger"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SchemaEventPayload) GetOptionalSchema(ctx context.Context) map[string]typing.KindDetails {
	fieldsObject := s.Schema.GetSchemaFromLabel(cdc.After)
	if fieldsObject == nil {
		// AFTER schema does not exist.
		return nil
	}

	schema := make(map[string]typing.KindDetails)
	for _, field := range fieldsObject.Fields {
		kd := field.ToKindDetails()
		if kd == typing.Invalid {
			logger.FromContext(ctx).WithFields(map[string]interface{}{
				"field": field.FieldName,
			}).Warn("skipping field from optional schema b/c we cannot determine the data type")
			continue
		}

		schema[field.FieldName] = kd
	}

	return schema
}
