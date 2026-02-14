package jsonutil

import (
	"encoding/json"
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
	{
		// Numbers are preserved as json.Number, not float64
		val, err := UnmarshalPayload(`{"count": 42}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"count": json.Number("42")}, val)
	}
}

func TestUnmarshal_BigIntPrecision(t *testing.T) {
	// This is the core precision bug: when unmarshalling large int64 values via standard json.Unmarshal
	// into map[string]any, they become float64 and lose precision (float64 only has 53 bits of mantissa).
	// With UseNumber, they are preserved as json.Number strings and can be precisely converted to int64.
	var result map[string]any
	err := Unmarshal([]byte(`{"big_int_test": 9223372036854775806}`), &result)
	assert.NoError(t, err)

	num, ok := result["big_int_test"].(json.Number)
	assert.True(t, ok, "expected json.Number, got %T", result["big_int_test"])

	val, err := num.Int64()
	assert.NoError(t, err)
	assert.Equal(t, int64(9223372036854775806), val)
}
