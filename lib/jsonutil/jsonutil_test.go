package jsonutil

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizePayload(t *testing.T) {
	{
		// Don't touch if the value isn't a string
		val, err := SanitizePayload(123)
		assert.NoError(t, err)
		assert.Equal(t, 123, val)
	}
	{
		// Invalid JSON string
		_, err := SanitizePayload("hello")
		assert.ErrorContains(t, err, "invalid character 'h' looking for beginning of value")
	}
	{
		// Valid JSON string, nothing changed.
		val, err := SanitizePayload(`{"hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"hello":"world"}`, val)
	}
	{
		// Fake JSON - appears to be in JSON format, but has duplicate keys
		val, err := SanitizePayload(`{"hello":"11world","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"hello":"world"}`, val)
	}
	{
		// Make sure all the keys are good and only duplicate keys got stripped
		val, err := SanitizePayload(`{"hello":"world","foo":"bar","hello":"world"}`)
		assert.NoError(t, err)

		var jsonMap map[string]any
		err = json.Unmarshal([]byte(fmt.Sprint(val)), &jsonMap)
		assert.NoError(t, err)

		assert.Contains(t, jsonMap, "hello")
		assert.Contains(t, jsonMap, "foo")
	}
}
