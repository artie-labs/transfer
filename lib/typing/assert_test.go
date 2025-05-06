package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertType(t *testing.T) {
	{
		// String to string
		val, err := AssertType[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Int to string
		_, err := AssertType[string](1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// Boolean to boolean
		val, err := AssertType[bool](true)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// String to boolean
		_, err := AssertType[bool]("true")
		assert.ErrorContains(t, err, "expected type bool, got string")
	}
}

func TestAssertTypeOptional(t *testing.T) {
	{
		// String to string
		val, err := AssertTypeOptional[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Nil to string
		val, err := AssertTypeOptional[string](nil)
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}
}
