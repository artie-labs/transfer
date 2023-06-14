package debezium

import (
	"github.com/artie-labs/transfer/lib/cdc"
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

// IsInteger inspects the field object within the schema object, a field is classified as an int
// When the "type" is int32 or int64. It also should not have a name (as that's where DBZ specify the data types)
func (f *Field) IsInteger() (valid bool) {
	if f == nil {
		return
	}

	validIntegerType := f.Type == "int16" || f.Type == "int32" || f.Type == "int64"
	return validIntegerType && f.DebeziumType == ""
}
