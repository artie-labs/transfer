package debezium

import (
	"encoding/json"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestField_IsInteger(t *testing.T) {
	payload := `
{
	"type": "struct",
	"fields": [{
		"type": "struct",
		"fields": [{
			"type": "int32",
			"optional": false,
			"default": 0,
			"field": "id"
		}, {
			"type": "string",
			"optional": false,
			"field": "first_name"
		}, {
			"type": "string",
			"optional": false,
			"field": "last_name"
		}, {
			"type": "string",
			"optional": false,
			"field": "email"
		}],
		"optional": true,
		"name": "dbserver1.inventory.customers.Value",
		"field": "after"
	}],
	"optional": false,
	"name": "dbserver1.inventory.customers.Envelope",
	"version": 1
}
`

	var schema Schema
	err := json.Unmarshal([]byte(payload), &schema)
	assert.NoError(t, err)

	var checked bool
	for _, field := range schema.GetSchemaFromLabel(cdc.After).Fields {
		if field.FieldName == "id" {
			assert.True(t, field.IsInteger())
			checked = true
		} else {
			assert.False(t, field.IsInteger())
		}
	}

	assert.True(t, checked)
}
