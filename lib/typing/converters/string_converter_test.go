package converters

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestGetStringConverter(t *testing.T) {
	{
		// Boolean
		converter, err := GetStringConverter(typing.Boolean, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, BooleanConverter{}, converter)
	}
	{
		// String
		converter, err := GetStringConverter(typing.String, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, StringConverter{}, converter)
	}
	{
		// Date
		converter, err := GetStringConverter(typing.Date, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, DateConverter{}, converter)
	}
	{
		// Time
		converter, err := GetStringConverter(typing.Time, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, TimeConverter{}, converter)
	}
	{
		// TimestampNTZ
		converter, err := GetStringConverter(typing.TimestampNTZ, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, TimestampNTZConverter{}, converter)
	}
	{
		// TimestampTZ
		converter, err := GetStringConverter(typing.TimestampTZ, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, TimestampTZConverter{}, converter)
	}
	{
		// Array
		converter, err := GetStringConverter(typing.Array, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, ArrayConverter{}, converter)
	}
	{
		// Struct
		converter, err := GetStringConverter(typing.Struct, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, StructConverter{}, converter)
	}
	{
		// EDecimal
		converter, err := GetStringConverter(typing.EDecimal, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, DecimalConverter{}, converter)
	}
	{
		// Integer
		converter, err := GetStringConverter(typing.Integer, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, IntegerConverter{}, converter)
	}
	{
		// Float
		converter, err := GetStringConverter(typing.Float, GetStringConverterOpts{})
		assert.NoError(t, err)
		assert.IsType(t, FloatConverter{}, converter)
	}
	{
		// Invalid
		converter, err := GetStringConverter(typing.Invalid, GetStringConverterOpts{})
		assert.ErrorContains(t, err, `unsupported type: "invalid"`)
		assert.Nil(t, converter)
	}
}

func TestBooleanConverter_Convert(t *testing.T) {
	{
		// Not boolean
		_, err := BooleanConverter{}.Convert("foo")
		assert.ErrorContains(t, err, `unexpected value: 'foo', type: string`)

		// Should be a ParseError with UnexpectedBooleanValue kind
		parseError, ok := typing.BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, typing.InvalidBooleanValue, parseError.GetKind())
	}
	{
		// True
		for _, possibleValue := range []any{1, true, "1", "true", "TRUE", "True"} {
			val, err := BooleanConverter{}.Convert(possibleValue)
			assert.NoError(t, err)
			assert.Equal(t, "true", val)
		}
	}
	{
		// False
		for _, possibleValue := range []any{0, false, "0", "false", "FALSE", "False"} {
			val, err := BooleanConverter{}.Convert(possibleValue)
			assert.NoError(t, err)
			assert.Equal(t, "false", val)
		}
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
		_, err := FloatConverter{}.Convert(true)
		assert.ErrorContains(t, err, `unexpected value: 'true', type: bool`)

		// Should be a ParseError with UnexpectedValue kind
		parseError, ok := typing.BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, typing.UnexpectedValue, parseError.GetKind())
	}
	{
		// Invalid string that can't be parsed as a float
		_, err := FloatConverter{}.Convert("tNLc2OHz")
		assert.ErrorContains(t, err, `unexpected value: 'tNLc2OHz', type: string`)

		// Should be a ParseError with UnexpectedValue kind
		parseError, ok := typing.BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, typing.UnexpectedValue, parseError.GetKind())
	}
	{
		// String
		val, err := FloatConverter{}.Convert("123.45")
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)
	}
	{
		// String with scientific notation
		val, err := FloatConverter{}.Convert("1.23e10")
		assert.NoError(t, err)
		assert.Equal(t, "1.23e10", val)
	}
	{
		// Negative string
		val, err := FloatConverter{}.Convert("-123.45")
		assert.NoError(t, err)
		assert.Equal(t, "-123.45", val)
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
		// *decimal.Decimal
		val, err := FloatConverter{}.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123.45")))
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
		for _, val := range []any{42, int8(42), int16(42), int32(42), int64(42), float32(42), float64(42), "42"} {
			parsedVal, err := IntegerConverter{}.Convert(val)
			assert.NoError(t, err)
			assert.Equal(t, "42", parsedVal)
		}
	}
	{
		// Test decimal.Decimal
		val, err := IntegerConverter{}.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123")))
		assert.NoError(t, err)
		assert.Equal(t, "123", val)
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
	{
		// Valid numeric strings
		for _, input := range []string{"123.45", "-123.45", "1.23e10", "42"} {
			val, err := DecimalConverter{}.Convert(input)
			assert.NoError(t, err)
			assert.Equal(t, input, val)
		}
	}
	{
		// Large decimal strings that exceed float64 range but are valid for NUMERIC
		// These would fail with strconv.ParseFloat (ErrRange) but should work with apd.NewFromString
		for _, input := range []string{
			"99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", // 309 nines
			"123456789012345678901234567890.123456789012345678901234567890", // high precision
		} {
			val, err := DecimalConverter{}.Convert(input)
			assert.NoError(t, err, "input: %s", input)
			assert.Equal(t, input, val)
		}
	}
	{
		// Invalid string that can't be parsed as a number
		_, err := DecimalConverter{}.Convert("tNLc2OHz")
		assert.ErrorContains(t, err, `unexpected value: 'tNLc2OHz', type: string`)

		// Should be a ParseError with UnexpectedValue kind
		parseError, ok := typing.BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, typing.UnexpectedValue, parseError.GetKind())
	}
	{
		// Unexpected type
		_, err := DecimalConverter{}.Convert(true)
		assert.ErrorContains(t, err, `unexpected value: 'true', type: bool`)

		// Should be a ParseError with UnexpectedValue kind
		parseError, ok := typing.BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, typing.UnexpectedValue, parseError.GetKind())
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
		// Struct
		val, err := StructConverter{}.Convert(`{"foo":"bar"}`)
		assert.NoError(t, err)
		assert.Equal(t, `"{\"foo\":\"bar\"}"`, val)
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
	{
		// Number
		val, err := StructConverter{}.Convert(123)
		assert.NoError(t, err)
		assert.Equal(t, "123", val)
	}
}

func TestStringConverter_Convert(t *testing.T) {
	conv := StringConverter{useNewMethod: true}
	{
		// String
		val, err := conv.Convert("foo")
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{
		// Boolean
		val, err := conv.Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, "true", val)
	}
	{
		// time.Time
		val, err := conv.Convert(time.Date(2021, 1, 1, 2, 3, 4, 5678910, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, "2021-01-01T02:03:04.00567891Z", val)
	}
	{
		// Integers
		for _, value := range []any{42, int8(42), int16(42), int32(42), int64(42), float32(42), float64(42)} {
			val, err := conv.Convert(value)
			assert.NoError(t, err)
			assert.Equal(t, "42", val)
		}
	}
	{
		// Floats
		for _, value := range []any{123.45, float32(123.45), float64(123.45)} {
			val, err := conv.Convert(value)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
	}
	{
		// Decimal
		val, err := conv.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123.45")))
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)
	}
	{
		// JSON
		val, err := conv.Convert(map[string]any{"foo": "bar"})
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		var obj any
		assert.NoError(t, json.Unmarshal([]byte(val), &obj))
		assert.Equal(t, map[string]any{"foo": "bar"}, obj)
	}
	{
		// Array
		val, err := conv.Convert([]string{"foo", "bar"})
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)

		var arr any
		assert.NoError(t, json.Unmarshal([]byte(val), &arr))
		assert.Equal(t, []any{"foo", "bar"}, arr.([]any))
	}
}

func TestTimestampNTZConverter_Convert(t *testing.T) {
	_time := time.Date(2023, 4, 24, 17, 29, 5, 699_000_000, time.UTC)
	value, err := NewTimestampNTZConverter("").Convert(_time)
	assert.NoError(t, err)
	assert.Equal(t, "2023-04-24T17:29:05.699", value)
}
