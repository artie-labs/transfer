package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringPassthrough_Convert(t *testing.T) {
	{
		// Non string
		_, err := StringPassthrough{}.Convert(1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// String
		value, err := StringPassthrough{}.Convert("test")
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
}
