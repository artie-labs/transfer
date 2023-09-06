package debezium

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/typing"
)

type Schema struct {
	SchemaType   string         `json:"type"`
	FieldsObject []FieldsObject `json:"fields"`
}

func (s *Schema) GetSchemaFromLabel(kind cdc.FieldLabelKind) *FieldsObject {
	for _, fieldObject := range s.FieldsObject {
		if fieldObject.FieldLabel == kind {
			return &fieldObject
		}
	}

	return nil
}

type FieldsObject struct {
	// What the type is for the block of field, e.g. STRUCT, or STRING.
	FieldObjectType string `json:"type"`

	// The actual schema object.
	Fields []Field `json:"fields"`

	// Whether this block for "after", "before", exists
	Optional   bool               `json:"optional"`
	FieldLabel cdc.FieldLabelKind `json:"field"`
}

type Field struct {
	Type         string                 `json:"type"`
	Optional     bool                   `json:"optional"`
	Default      interface{}            `json:"default"`
	FieldName    string                 `json:"field"`
	DebeziumType string                 `json:"name"`
	Parameters   map[string]interface{} `json:"parameters"`
}

func (f Field) IsInteger() (valid bool) {
	return f.ToKindDetails() == typing.Integer
}

func (f Field) ToKindDetails() typing.KindDetails {
	switch f.Type {
	case "int16", "int32", "int64":
		if f.DebeziumType == "" {
			return typing.Integer
		}
		// TODO: deal with ts.

		return typing.Integer
	case "float", "double":
		// TODO: custom-snapshot is not emitting this yet.
		return typing.Float
	case "string":
		// Within string, now inspect the Debezium type
		// TODO: Also deal with strings
		return typing.String
	case "struct":
		return typing.Struct
	case "boolean":
		return typing.Boolean
	case "array":
		return typing.Array
	default:
		return typing.Invalid
	}
}
