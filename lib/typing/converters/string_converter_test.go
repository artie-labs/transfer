package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestBooleanConverter_Convert(t *testing.T) {
	{
		// Not boolean
		_, err := BooleanConverter{}.Convert("foo")
		assert.ErrorContains(t, err, `failed to cast colVal as boolean, colVal: 'foo', type: string`)
	}
	{
		// True
		val, err := BooleanConverter{}.Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, "true", val)
	}
	{
		// False
		val, err := BooleanConverter{}.Convert(false)
		assert.NoError(t, err)
		assert.Equal(t, "false", val)
	}
}

func TestArrayConverter_Convert(t *testing.T) {
	// Array
	{
		// Normal arrays
		val, err := ArrayConverter{}.Convert([]string{"foo", "bar"})
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)
	}
	{
		// Toasted array
		val, err := ArrayConverter{}.Convert(constants.ToastUnavailableValuePlaceholder)
		assert.NoError(t, err)
		assert.Equal(t, `["__debezium_unavailable_value"]`, val)
	}
}

func TestFloatConverter_Convert(t *testing.T) {
	{
		// Unexpected type
		_, err := FloatConverter{}.Convert("foo")
		assert.ErrorContains(t, err, `unexpected value: 'foo', type: string`)
	}
	{
		// Float32
		val, err := FloatConverter{}.Convert(float32(123.45))
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)
	}
	{
		// Float64
		val, err := FloatConverter{}.Convert(float64(123.45))
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)
	}
	{
		// Integers
		for _, input := range []any{42, int8(42), int16(42), int32(42), int64(42), float32(42), float64(42)} {
			val, err := FloatConverter{}.Convert(input)
			assert.NoError(t, err)
			assert.Equal(t, "42", val)
		}
	}
}

func TestIntegerConverter_Convert(t *testing.T) {
	{
		// Various numbers
		for _, val := range []any{42, int8(42), int16(42), int32(42), int64(42), float32(42), float64(42)} {
			parsedVal, err := IntegerConverter{}.Convert(val)
			assert.NoError(t, err)
			assert.Equal(t, "42", parsedVal)
		}
	}
	{
		// Booleans
		{
			// True
			val, err := IntegerConverter{}.Convert(true)
			assert.NoError(t, err)
			assert.Equal(t, "1", val)
		}
		{
			// False
			val, err := IntegerConverter{}.Convert(false)
			assert.NoError(t, err)
			assert.Equal(t, "0", val)
		}
	}
}

func TestDecimalConverter_Convert(t *testing.T) {
	{
		// Extended decimal
		val, err := DecimalConverter{}.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123.45")))
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)
	}
	{
		// Floats
		for _, input := range []any{float32(123.45), float64(123.45)} {
			val, err := DecimalConverter{}.Convert(input)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
	}
	{
		// Integers
		for _, input := range []any{42, int8(42), int16(42), int32(42), int64(42), float32(42), float64(42)} {
			val, err := DecimalConverter{}.Convert(input)
			assert.NoError(t, err)
			assert.Equal(t, "42", val)
		}
	}
}

func TestStructConverter_Convert(t *testing.T) {
	{
		// Toast
		val, err := StructConverter{}.Convert(constants.ToastUnavailableValuePlaceholder)
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, val)
	}
	{
		// Toast object
		val, err := StructConverter{}.Convert(`{"__debezium_unavailable_value":"__debezium_unavailable_value"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, val)
	}
	{
		// Normal string
		val, err := StructConverter{}.Convert(`{"foo":"bar"}`)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)
	}
	{
		// Boolean
		val, err := StructConverter{}.Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, "true", val)
	}
	{
		// Map
		val, err := StructConverter{}.Convert(map[string]any{"foo": "bar"})
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)
	}
}
