package parquetutil

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestParseValueForArrow(t *testing.T) {
	{
		// Nil
		value, err := ParseValueForArrow(nil, typing.KindDetails{})
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// String
		value, err := ParseValueForArrow("test", typing.String)
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Struct - now returns formatted string instead of JSON
		value, err := ParseValueForArrow(map[string]any{"foo": "bar"}, typing.Struct)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, value)
	}
	{
		// Array - now returns formatted string instead of array
		value, err := ParseValueForArrow([]int{123, 456}, typing.Array)
		assert.NoError(t, err)
		assert.Equal(t, []string{"123", "456"}, value)
	}
	{
		// Array boolean - now returns formatted string instead of array
		value, err := ParseValueForArrow([]bool{false, true, false}, typing.Array)
		assert.NoError(t, err)
		assert.Equal(t, []string{"false", "true", "false"}, value)
	}
	{
		// Integer
		value, err := ParseValueForArrow(int64(123), typing.Integer)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), value)
	}
	{
		// Integer from string
		value, err := ParseValueForArrow("456", typing.Integer)
		assert.NoError(t, err)
		assert.Equal(t, int64(456), value)
	}
	{
		// Boolean
		value, err := ParseValueForArrow(true, typing.Boolean)
		assert.NoError(t, err)
		assert.Equal(t, true, value)
	}
	{
		// Float
		value, err := ParseValueForArrow(float32(3.14), typing.Float)
		assert.NoError(t, err)
		assert.Equal(t, float32(3.14), value)
	}
	{
		// Decimal with valid precision
		decimalDetails := decimal.NewDetails(10, 2)
		decimalKind := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimalDetails)
		decimalValue := decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("123.45"), 10)

		value, err := ParseValueForArrow(decimalValue, decimalKind)
		assert.NoError(t, err)

		// Should be decimal128.Num
		if _, ok := value.(decimal128.Num); !ok {
			// If decimal128 conversion fails, it should fallback to string
			assert.Equal(t, "123.45", value)
		}
	}
	{
		// Time from string
		value, err := ParseValueForArrow("12:30:45", typing.TimeKindDetails)
		assert.NoError(t, err)

		// Should be milliseconds since midnight: (12*3600 + 30*60 + 45) * 1000
		expectedMillis := int32((12*3600 + 30*60 + 45) * 1000)
		assert.Equal(t, expectedMillis, value)
	}
	{
		// Date from string
		value, err := ParseValueForArrow("2023-12-25", typing.Date)
		assert.NoError(t, err)

		// Should be days since epoch
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		expectedDate := time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC)
		expectedDays := int32(expectedDate.Sub(epoch).Hours() / 24)
		assert.Equal(t, expectedDays, value)
	}
	{
		// Timestamp from string
		value, err := ParseValueForArrow("2023-12-25T10:30:00Z", typing.TimestampTZ)
		assert.NoError(t, err)

		// Should be milliseconds since epoch
		expectedTime, _ := time.Parse(time.RFC3339, "2023-12-25T10:30:00Z")
		expectedMillis := expectedTime.UnixMilli()
		assert.Equal(t, expectedMillis, value)
	}
}

func TestConvertValueForArrowBuilder_TypedNils(t *testing.T) {
	pool := memory.NewGoAllocator()
	{
		// Typed nil []byte should append null, not empty string
		builder := array.NewStringBuilder(pool)
		defer builder.Release()

		var nilBytes []byte = nil
		assert.NoError(t, ConvertValueForArrowBuilder(builder, nilBytes))
		arr := builder.NewStringArray()
		defer arr.Release()

		assert.Equal(t, 1, arr.Len())
		assert.True(t, arr.IsNull(0), "typed nil []byte should result in null")
	}
	{
		// Typed nil map[string]any should append null, not "null" string
		builder := array.NewStringBuilder(pool)
		defer builder.Release()

		var nilMap map[string]any = nil
		assert.NoError(t, ConvertValueForArrowBuilder(builder, nilMap))
		arr := builder.NewStringArray()
		defer arr.Release()

		assert.Equal(t, 1, arr.Len())
		assert.True(t, arr.IsNull(0), "typed nil map should result in null")
	}
	{
		// Typed nil []any should append null
		builder := array.NewStringBuilder(pool)
		defer builder.Release()

		var nilSlice []any = nil
		assert.NoError(t, ConvertValueForArrowBuilder(builder, nilSlice))
		arr := builder.NewStringArray()
		defer arr.Release()

		assert.Equal(t, 1, arr.Len())
		assert.True(t, arr.IsNull(0), "typed nil []any should result in null")
	}
	{
		// Non-nil values should still work
		builder := array.NewStringBuilder(pool)
		defer builder.Release()

		assert.NoError(t, ConvertValueForArrowBuilder(builder, []byte("hello")))
		assert.NoError(t, ConvertValueForArrowBuilder(builder, map[string]any{"key": "value"}))
		assert.NoError(t, ConvertValueForArrowBuilder(builder, []any{1, 2, 3}))
		assert.NoError(t, ConvertValueForArrowBuilder(builder, "test"))
		arr := builder.NewStringArray()
		defer arr.Release()

		assert.Equal(t, 4, arr.Len())
		assert.False(t, arr.IsNull(0))
		assert.Equal(t, "hello", arr.Value(0))
		assert.False(t, arr.IsNull(1))
		assert.Equal(t, `{"key":"value"}`, arr.Value(1))
		assert.False(t, arr.IsNull(2))
		assert.Equal(t, `[1,2,3]`, arr.Value(2))
		assert.False(t, arr.IsNull(3))
		assert.Equal(t, "test", arr.Value(3))
	}
}

func TestConvertValueForArrowBuilder_UntypedNil(t *testing.T) {
	pool := memory.NewGoAllocator()
	builder := array.NewInt64Builder(pool)
	defer builder.Release()

	assert.NoError(t, ConvertValueForArrowBuilder(builder, nil))
	arr := builder.NewArray().(*array.Int64)
	defer arr.Release()

	assert.Equal(t, 1, arr.Len())
	assert.True(t, arr.IsNull(0))
}
