package converters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestStringConverter_Convert(t *testing.T) {
	converter := StringConverter{}
	{
		// String
		val, err := converter.Convert("foo")
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{
		// Decimal
		val, err := converter.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123")))
		assert.NoError(t, err)
		assert.Equal(t, "123", val)
	}
	{
		// Boolean
		val, err := converter.Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, "true", val)
	}
	{
		// Invalid
		_, err := converter.Convert(123)
		assert.ErrorContains(t, err, "expected string/*decimal.Decimal/bool received int with value 123")
	}
	{
		// Extended time
		val, err := converter.Convert(ext.NewExtendedTime(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), ext.TimestampTZKindType, ""))
		assert.NoError(t, err)
		assert.Equal(t, "2021-01-01T00:00:00Z", val)
	}
}

func TestInt64Converter_Convert(t *testing.T) {
	converter := Int64Converter{}
	{
		// int
		val, err := converter.Convert(123)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), val)
	}
	{
		// int32
		val, err := converter.Convert(int32(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), val)
	}
	{
		// int64
		val, err := converter.Convert(int64(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), val)
	}
	{
		// Invalid
		_, err := converter.Convert("foo")
		assert.ErrorContains(t, err, "expected int/int32/int64 received string with value foo")
	}
}

func TestBooleanConverter_Convert(t *testing.T) {
	converter := BooleanConverter{}
	{
		// bool
		val, err := converter.Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// String
		val, err := converter.Convert("true")
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// Invalid
		_, err := converter.Convert(123)
		assert.ErrorContains(t, err, "expected bool received int with value 123")
	}
}

func TestFloat64Converter_Convert(t *testing.T) {
	converter := Float64Converter{}
	{
		// Float32
		val, err := converter.Convert(float32(123))
		assert.NoError(t, err)
		assert.Equal(t, float64(123), val)
	}
	{
		// Float64
		val, err := converter.Convert(float64(123.45))
		assert.NoError(t, err)
		assert.Equal(t, float64(123.45), val)
	}
	{
		// Int32
		val, err := converter.Convert(int32(123))
		assert.NoError(t, err)
		assert.Equal(t, float64(123), val)
	}
	{
		// Int64
		val, err := converter.Convert(int64(123))
		assert.NoError(t, err)
		assert.Equal(t, float64(123), val)
	}
	{
		// *decimal.Decimal
		val, err := converter.Convert(decimal.NewDecimal(numbers.MustParseDecimal("123.45")))
		assert.NoError(t, err)
		assert.Equal(t, float64(123.45), val)
	}
	{
		// String
		{
			// Invalid
			_, err := converter.Convert("foo")
			assert.Errorf(t, err, "failed to parse string")
		}
		{
			// Valid
			val, err := converter.Convert("123.45")
			assert.NoError(t, err)
			assert.Equal(t, float64(123.45), val)
		}
	}
	{
		// Not supported type
		_, err := converter.Convert(true)
		assert.ErrorContains(t, err, "failed to run float64 converter, unexpected type bool with value true")
	}
}
