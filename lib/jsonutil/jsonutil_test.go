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
}
