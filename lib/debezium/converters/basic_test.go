package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func TestJSON_Convert(t *testing.T) {
	{
		// Wrong data type
		_, err := JSON{}.Convert(123)
		assert.ErrorContains(t, err, "expected string, got int")
	}
	{
		// JSON with duplicate values
		value, err := JSON{}.Convert(`{"a": 1, "a": 2}`)
		assert.NoError(t, err)
		assert.Equal(t, map[string]any{"a": float64(2)}, value)
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
		// Irrelevant data type
		value, err := Array{}.Convert([]int{1, 2, 3, 4})
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3, 4}, value)
	}
	{
		// TOASTED data
		{
			// As []any
			value, err := Array{}.Convert([]any{constants.ToastUnavailableValuePlaceholder})
			assert.NoError(t, err)
			assert.Equal(t, constants.ToastUnavailableValuePlaceholder, value)
		}
		{
			// As []string
			value, err := Array{}.Convert([]string{constants.ToastUnavailableValuePlaceholder})
			assert.NoError(t, err)
			assert.Equal(t, constants.ToastUnavailableValuePlaceholder, value)
		}
	}
	{
		// Array of JSON objects
		{
			// Invalid json
			_, err := NewArray(JSON{}.Convert).Convert([]any{"hello"})
			assert.Error(t, err)
		}
		{
			// Invalid data type
			_, err := NewArray(JSON{}.Convert).Convert([]any{123})
			assert.ErrorContains(t, err, "expected string, got int")
		}
		{
			// Valid
			value, err := NewArray(JSON{}.Convert).Convert([]any{`{"body": "they are on to us", "sender": "pablo"}`})
			assert.NoError(t, err)
			assert.Len(t, value.([]any), 1)
			assert.ElementsMatch(t, []any{map[string]any{"body": "they are on to us", "sender": "pablo"}}, value.([]any))
		}
	}
}
