package jsonutil

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizePayload(t *testing.T) {
	{
		// Invalid JSON string
		_, err := SanitizePayload("hello")
		assert.Error(t, err)
	}
	{
		// Valid JSON string, nothing changed.
		val, err := SanitizePayload(`{"hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"hello":"world"}`, val)
	}
	{
		// Fake JSON - appears to be in JSON format, but has duplicate keys
		val, err := SanitizePayload(`{"hello":"world","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"hello":"world"}`, val)
	}
	{
		// Make sure all the keys are good and only duplicate keys got stripped
		val, err := SanitizePayload(`{"hello":"world","foo":"bar","hello":"world"}`)
		assert.NoError(t, err)

		var jsonMap map[string]interface{}
		err = json.Unmarshal([]byte(fmt.Sprint(val)), &jsonMap)
		assert.NoError(t, err)

		var foundHello bool
		var foundFoo bool
		for key := range jsonMap {
			if key == "hello" {
				foundHello = true
			}
			if key == "foo" {
				foundFoo = true
			}
		}

		assert.True(t, foundHello)
		assert.True(t, foundFoo)
	}
}
