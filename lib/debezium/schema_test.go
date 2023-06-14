package debezium

import (
	"encoding/json"
	"testing"

	"github.com/artie-labs/transfer/lib/array"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/stretchr/testify/assert"
)

func TestField_IsInteger(t *testing.T) {
	payload := `{
	"type": "struct",
	"fields": [{
		"type": "struct",
		"fields": [{
			"type": "int16",
			"optional": true,
			"field": "smallint_test"
		}, {
			"type": "int16",
			"optional": false,
			"default": 0,
			"field": "smallserial_test"
		}, {
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
}`

	var schema Schema
	err := json.Unmarshal([]byte(payload), &schema)
	assert.NoError(t, err)

	integerKeys := []string{"id", "smallserial_test", "smallint_test"}
	var foundIntKeys []string
	var foundNonIntKeys []string

	for _, field := range schema.GetSchemaFromLabel(cdc.After).Fields {
		if field.IsInteger() {
			foundIntKeys = append(foundIntKeys, field.FieldName)
		} else {
			foundNonIntKeys = append(foundNonIntKeys, field.FieldName)
		}
	}

	assert.True(t, len(foundIntKeys) > 0)
	assert.True(t, len(foundNonIntKeys) > 0)

	for _, key := range foundIntKeys {
		// Make sure these flagged keys are specified within integerKeys.
		assert.True(t, array.StringContains(integerKeys, key))
	}

	for _, key := range foundNonIntKeys {
		// Make sure these flagged keys are specified within integerKeys.
		assert.False(t, array.StringContains(integerKeys, key))
	}
}
