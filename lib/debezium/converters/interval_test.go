package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func TestInterval_Convert(t *testing.T) {
	{
		// Non string
		_, err := Interval{}.Convert(1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// String
		value, err := Interval{}.Convert("P1Y2M3DT4H5M6.7S")
		assert.NoError(t, err)
		assert.Equal(t, "P1Y2M3DT4H5M6.7S", value)
	}
}

func TestInterval_ToKindDetails(t *testing.T) {
	assert.Equal(t, typing.Interval, Interval{}.ToKindDetails())
}
