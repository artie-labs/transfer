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

func TestDefaultValueFromPtr(t *testing.T) {
	{
		// ptr is not set
		assert.Equal(t, int32(5), DefaultValueFromPtr[int32](nil, int32(5)))
	}
	{
		// ptr is set
		assert.Equal(t, int32(10), DefaultValueFromPtr[int32](ToPtr(int32(10)), int32(5)))
	}
}
