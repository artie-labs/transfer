package jsonutil

import (
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
}
