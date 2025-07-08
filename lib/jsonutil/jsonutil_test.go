package jsonutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalPayload(t *testing.T) {
	{
		// Invalid JSON string
		_, err := UnmarshalPayload("hello")
		assert.Error(t, err)
	}
	{
		// Empty JSON string edge case
		val, err := UnmarshalPayload("")
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}
	{
		// Valid JSON string, nothing changed.
		val, err := UnmarshalPayload(`{"hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world"}, val)
	}
	{
		// Fake JSON - appears to be in JSON format, but has duplicate keys
		val, err := UnmarshalPayload(`{"hello":"11world","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world"}, val)
	}
	{
		// Make sure all the keys are good and only duplicate keys got stripped
		val, err := UnmarshalPayload(`{"hello":"world","foo":"bar","hello":"world"}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"hello": "world", "foo": "bar"}, val)
	}
}
