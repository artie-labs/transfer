package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString_Convert(t *testing.T) {
	{
		// Non string
		_, err := String{}.Convert(1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// String
		value, err := String{}.Convert("test")
		assert.Nil(t, err)
		assert.Equal(t, "test", value)
	}
}
