package jsonutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePayload(t *testing.T) {
	{
		// Invalid JSON string
		_, err := ParsePayload("hello")
		assert.ErrorContains(t, err, "invalid character 'h' looking for beginning of value")
	}
	{
		// Empty JSON string edge case
		val, err := ParsePayload("")
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}
	{
		// Valid JSON string, nothing changed.
		val, err := ParsePayload(`{"hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world"}, val)
	}
	{
		// Fake JSON - appears to be in JSON format, but has duplicate keys
		val, err := ParsePayload(`{"hello":"11world","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world"}, val)
	}
	{
		// Make sure all the keys are good and only duplicate keys got stripped
		val, err := ParsePayload(`{"hello":"world","foo":"bar","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world", "foo": "bar"}, val)
	}
}
