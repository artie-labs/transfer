package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSON_Convert(t *testing.T) {
	{
		// Wrong data type
		value, err := JSON{}.Convert(123)
		assert.Nil(t, value)
		assert.ErrorContains(t, err, "expected string, got int")
	}
	{
		// JSON with duplicate values
		value, err := JSON{}.Convert(`{"a": 1, "a": 2}`)
		assert.Nil(t, err)
		assert.Equal(t, `{"a":2}`, value)
	}
}

func TestYear_Convert(t *testing.T) {
	{
		// Wrong data type
		_, err := Year{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		//	Valid data type
		value, err := Year{}.Convert(int64(2024))
		assert.NoError(t, err)
		assert.Equal(t, int64(2024), value)
	}
}
