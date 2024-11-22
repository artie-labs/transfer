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

func TestInt64Passthrough_Convert(t *testing.T) {
	{
		// Wrong data type
		_, err := Int64Passthrough{}.Convert("123")
		assert.ErrorContains(t, err, "expected type int64, got string")
	}
	{
		//	Valid data type
		value, err := Int64Passthrough{}.Convert(int64(2024))
		assert.NoError(t, err)
		assert.Equal(t, int64(2024), value)
	}
}

func TestBase64_Convert(t *testing.T) {
	{
		// Wrong data type
		_, err := Base64{}.Convert("123")
		assert.ErrorContains(t, err, "expected type []uint8, got string")
	}
	{
		// Valid data type
		value, err := Base64{}.Convert([]byte("2024"))
		assert.NoError(t, err)
		assert.Equal(t, "MjAyNA==", value)
	}
}

func TestFloat64_Convert(t *testing.T) {
	{
		// Invalid
		{
			// Wrong data type
			_, err := Float64{}.Convert("123")
			assert.ErrorContains(t, err, `unexpected type string, with value "123"`)
		}
		{
			// Another wrong data type
			_, err := Float64{}.Convert(false)
			assert.ErrorContains(t, err, "unexpected type bool")
		}
	}
	{
		// Valid
		{
			// int
			value, err := Float64{}.Convert(123)
			assert.NoError(t, err)
			assert.Equal(t, float64(123), value)
		}
		{
			// NaN
			value, err := Float64{}.Convert("NaN")
			assert.NoError(t, err)
			assert.Nil(t, value)
		}
		{
			// Float
			value, err := Float64{}.Convert(123.45)
			assert.NoError(t, err)
			assert.Equal(t, 123.45, value)
		}
	}
}

func TestArray_Convert(t *testing.T) {
	{
		// Valid
		value, err := Array{}.Convert([]int{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, value)
	}
	{
		// Filter TOASTED value
		value, err := Array{}.Convert([]string{"__debezium_unavailable_value"})
		assert.NoError(t, err)
		assert.Equal(t, "__debezium_unavailable_value", value)
	}
}
