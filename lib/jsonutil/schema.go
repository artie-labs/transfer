package jsonutil

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
)

type JSONSchema struct {
	Type                 string                 `json:"type"`
	Properties           map[string]interface{} `json:"properties,omitempty"`
	Items                *JSONSchema            `json:"items,omitempty"`
	AdditionalProperties bool                   `json:"additionalProperties"`
}

func SchemaFromColumns(ctx context.Context, cols []columns.Column) (string, error) {
	schema := &JSONSchema{
		Type:                 "object",
		Properties:           make(map[string]interface{}),
		AdditionalProperties: false,
	}

	for _, col := range cols {
		var prop *JSONSchema
		switch col.KindDetails.Kind {
		case typing.Float.Kind, typing.EDecimal.Kind:
			prop = &JSONSchema{Type: "number"}
		case "int":
			prop = &JSONSchema{Type: "integer"}
		case "bool":
			prop = &JSONSchema{Type: "boolean"}
		case "array":
			prop = &JSONSchema{Type: "array", Items: &JSONSchema{Type: "string"}} // Replace "string" with actual type of items in the array
		case "struct":
			// If structs can have arbitrary properties, use `AdditionalProperties: true`.
			// If they have a fixed schema, define it in `Properties`.
			prop = &JSONSchema{Type: "object", AdditionalProperties: true}
		case "string", "extended_time":
			prop = &JSONSchema{Type: "string"}
		default:
			return "", fmt.Errorf("unknown kind: %s", col.KindDetails.Kind)
		}

		// We don't need to escape col names.
		schema.Properties[col.Name(ctx, nil)] = prop
	}

	jsonSchema, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}

	return string(jsonSchema), nil
}
