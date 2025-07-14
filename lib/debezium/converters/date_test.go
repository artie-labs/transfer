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
		val, err := Date{}.Convert(int64(19401))
		assert.NoError(t, err)
		assert.Equal(t, "2023-02-13", val.(string))
	}
	{
		val, err := Date{}.Convert(int64(19429))
		assert.NoError(t, err)
		assert.Equal(t, "2023-03-13", val.(string))
	}
	{
		// Invalid date (year exceeds 9999)
		val, err := Date{}.Convert(int64(10_000_000))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}
