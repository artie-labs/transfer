package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDate_Convert(t *testing.T) {
	{
		// Invalid data type
		_, err := Date{}.Convert("invalid")
		assert.ErrorContains(t, err, "expected int64 got 'invalid' with type string")
	}
	{

	}
}
