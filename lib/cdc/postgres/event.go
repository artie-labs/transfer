package postgres

import (
	"github.com/artie-labs/transfer/lib/cdc"
)

// SchemaEventPayload is our struct for an event with schema enabled. For reference, this is an example payload https://gist.github.com/Tang8330/3b9989ed8c659771958fe481f248397a
type SchemaEventPayload struct {
	Schema  schema  `json:"schema"`
	Payload payload `json:"payload"`
}

type schema struct {
	SchemaType   string         `json:"type"`
	FieldsObject []fieldsObject `json:"fields"`
}

func (s *schema) GetSchemaFromLabel(kind cdc.FieldLabelKind) *fieldsObject {
	for _, fieldObject := range s.FieldsObject {
		if fieldObject.FieldLabel == kind {
			return &fieldObject
		}
	}

	return nil
}

type fieldsObject struct {
	// What the type is for the block of field, e.g. STRUCT, or STRING.
	FieldObjectType string `json:"type"`

	// The actual schema object.
	Fields []Field `json:"fields"`

	// Whether this block for "after", "before", exists
	Optional   bool               `json:"optional"`
	FieldLabel cdc.FieldLabelKind `json:"field"`
}

type Field struct {
	Optional     bool        `json:"optional"`
	Default      interface{} `json:"default"`
	FieldName    string      `json:"field"`
	DebeziumType string      `json:"name"`
}

type payload struct {
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
	Source    Source                 `json:"source"`
	Operation string                 `json:"op"`
}

type Source struct {
	Connector string `json:"connector"`
	TsMs      int64  `json:"ts_ms"`
	Database  string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
}
