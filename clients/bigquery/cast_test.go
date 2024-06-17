package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeStructToJSONString(t *testing.T) {
	{
		// Empty string:
		result, err := EncodeStructToJSONString("")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	}
	{
		// Toasted string:
		result, err := EncodeStructToJSONString("__debezium_unavailable_value")
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, result)
	}
	{
		// Map:
		result, err := EncodeStructToJSONString(map[string]any{"foo": "bar", "baz": 1234})
		assert.NoError(t, err)
		assert.Equal(t, `{"baz":1234,"foo":"bar"}`, result)
	}
	{
		// Toasted map (should not happen):
		result, err := EncodeStructToJSONString(map[string]any{"__debezium_unavailable_value": "bar", "baz": 1234})
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, result)
	}
}
