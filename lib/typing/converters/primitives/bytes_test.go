package primitives

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsBytes(t *testing.T) {
	{
		// []byte input is returned as-is.
		input := []byte("hello")
		result, err := AsBytes(input)
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello"), result)
	}
	{
		// string input is converted to []byte.
		result, err := AsBytes("world")
		assert.NoError(t, err)
		assert.Equal(t, []byte("world"), result)
	}
	{
		// Empty string.
		result, err := AsBytes("")
		assert.NoError(t, err)
		assert.Equal(t, []byte(""), result)
	}
	{
		// Integer is JSON-marshalled.
		result, err := AsBytes(42)
		assert.NoError(t, err)
		assert.Equal(t, []byte("42"), result)
	}
	{
		// Float is JSON-marshalled.
		result, err := AsBytes(3.14)
		assert.NoError(t, err)
		assert.Equal(t, []byte("3.14"), result)
	}
	{
		// Boolean is JSON-marshalled.
		result, err := AsBytes(true)
		assert.NoError(t, err)
		assert.Equal(t, []byte("true"), result)
	}
	{
		// Map is JSON-marshalled.
		result, err := AsBytes(map[string]string{"key": "value"})
		assert.NoError(t, err)
		assert.Equal(t, []byte(`{"key":"value"}`), result)
	}
	{
		// Slice is JSON-marshalled.
		result, err := AsBytes([]int{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, []byte("[1,2,3]"), result)
	}
	{
		// nil is JSON-marshalled to "null".
		result, err := AsBytes(nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("null"), result)
	}
	{
		// Unmarshallable value (channel) returns an error.
		_, err := AsBytes(make(chan int))
		assert.ErrorContains(t, err, "failed to convert chan int to []byte")
	}
}
