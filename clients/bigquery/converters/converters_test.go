package converters

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestStringConverter_Convert(t *testing.T) {
	{
		// String
		val, err := NewStringConverter(typing.String).Convert("foo")
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{
		// Decimal
		val, err := NewStringConverter(typing.EDecimal).Convert(decimal.NewDecimal(numbers.MustParseDecimal("123")))
		assert.NoError(t, err)
		assert.Equal(t, "123", val)
	}
	{
		// Boolean
		val, err := NewStringConverter(typing.Boolean).Convert(true)
		assert.NoError(t, err)
		assert.Equal(t, "true", val)
	}
	{
		// Integer column
		{
			// int64 value
			val, err := NewStringConverter(typing.Integer).Convert(int64(123))
			assert.NoError(t, err)
			assert.Equal(t, "123", val)
		}
		{
			// int value
			val, err := NewStringConverter(typing.Integer).Convert(123)
			assert.NoError(t, err)
			assert.Equal(t, "123", val)
		}
	}
	{
		// float64
		{
			// 123.45
			val, err := NewStringConverter(typing.Float).Convert(float64(123.45))
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
		{
			// 123.123456789
			val, err := NewStringConverter(typing.Float).Convert(float64(123.123456789))
			assert.NoError(t, err)
			assert.Equal(t, "123.123456789", val)
		}
		{
			// Max float64 value
			val, err := NewStringConverter(typing.Float).Convert(math.MaxFloat64)
			assert.NoError(t, err)
			assert.Equal(t, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", val)
		}
		{
			// Min float64 value
			val, err := NewStringConverter(typing.Float).Convert(math.SmallestNonzeroFloat64)
			assert.NoError(t, err)
			assert.Equal(t, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005", val)
		}
	}
	{
		// time.Time
		{
			// Date
			val, err := NewStringConverter(typing.Date).Convert(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01", val)
		}
		{
			// Time
			val, err := NewStringConverter(typing.TimeKindDetails).Convert(time.Date(2021, 1, 1, 9, 10, 11, 123_456_789, time.UTC))
			assert.NoError(t, err)
			assert.Equal(t, "09:10:11.123456", val)
		}
		{
			// Timestamp NTZ
			val, err := NewStringConverter(typing.TimestampNTZ).Convert(time.Date(2021, 1, 1, 9, 10, 12, 400_123_991, time.UTC))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01T09:10:12.400123991", val)
		}
		{
			// Timestamp TZ
			val, err := NewStringConverter(typing.TimestampTZ).Convert(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01T00:00:00Z", val)
		}
		{
			// Invalid
			ts := time.Date(2021, 1, 1, 9, 10, 12, 400_123_991, time.UTC)
			val, err := NewStringConverter(typing.String).Convert(ts)
			assert.NoError(t, err)
			assert.Equal(t, ts.String(), val)
		}
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
		// float32
		val, err := converter.Convert(float32(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), val)
	}
	{
		// float64
		val, err := converter.Convert(float64(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), val)
	}
	{
		// *decimal.Decimal
		val, err := converter.Convert(decimal.NewDecimal(numbers.MustParseDecimal("100000000")))
		assert.NoError(t, err)
		assert.Equal(t, int64(100000000), val)
	}
	{
		// json.Number
		val, err := converter.Convert(json.Number("42"))
		assert.NoError(t, err)
		assert.Equal(t, int64(42), val)
	}
	{
		// json.Number - large int
		val, err := converter.Convert(json.Number("9223372036854775806"))
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775806), val)
	}
	{
		// Invalid
		_, err := converter.Convert("foo")
		assert.ErrorContains(t, err, "unexpected data type - received string with value foo")
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
		{
			// Empty string
			val, err := converter.Convert("")
			assert.NoError(t, err)
			assert.Nil(t, val)
		}
	}
	{
		// json.Number
		val, err := converter.Convert(json.Number("123.45"))
		assert.NoError(t, err)
		assert.Equal(t, float64(123.45), val)
	}
	{
		// json.Number - integer
		val, err := converter.Convert(json.Number("42"))
		assert.NoError(t, err)
		assert.Equal(t, float64(42), val)
	}
	{
		// Not supported type
		_, err := converter.Convert(true)
		assert.ErrorContains(t, err, "failed to run float64 converter, unexpected type bool with value true")
	}
}
