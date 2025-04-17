package converters

import (
	"testing"
	"time"

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

		date, isOk := val.(time.Time)
		assert.True(t, isOk)
		assert.Equal(t, "2023-02-13", date.Format(time.DateOnly))
	}
	{
		val, err := Date{}.Convert(int64(19429))
		assert.NoError(t, err)

		date, isOk := val.(time.Time)
		assert.True(t, isOk)
		assert.Equal(t, "2023-03-13", date.Format(time.DateOnly))
	}
}
