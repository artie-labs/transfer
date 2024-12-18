package values

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestFloat32ToString(t *testing.T) {
	type ioPair struct {
		input  float32
		output string
	}

	ioPairs := []ioPair{
		{123.456, "123.456"},
		{0.0, "0"},
		{-1.0, "-1"},
		{1.0, "1"},
		{340282350000000000000000000000000000000, "340282350000000000000000000000000000000"},
		{math.MaxFloat32, "340282350000000000000000000000000000000"},
		{0.000000000000000000000000000000000000000000001, "0.000000000000000000000000000000000000000000001"},
		{-340282350000000000000000000000000000000, "-340282350000000000000000000000000000000"},
		{1.401298464324817070923729583289916131280e-45, "0.000000000000000000000000000000000000000000001"},
		{1.17549435e-38, "0.000000000000000000000000000000000000011754944"},
		{-1.17549435e-38, "-0.000000000000000000000000000000000000011754944"},
		{2.71828, "2.71828"},
		{-2.71828, "-2.71828"},
		{3.14159, "3.14159"},
		{-3.14159, "-3.14159"},
	}

	for _, pair := range ioPairs {
		assert.Equal(t, pair.output, Float32ToString(pair.input), pair.input)
	}
}

func TestFloat64ToString(t *testing.T) {
	type ioPair struct {
		input  float64
		output string
	}

	ioPairs := []ioPair{
		{123.456, "123.456"},
		{0.0, "0"},
		{-1.0, "-1"},
		{1.0, "1"},
		{1.7976931348623157e+308, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{math.MaxFloat64, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{4.9406564584124654e-324, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005"},
		{-1.7976931348623157e+308, "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{2.718281828459045, "2.718281828459045"},
		{-2.718281828459045, "-2.718281828459045"},
		{3.141592653589793, "3.141592653589793"},
		{-3.141592653589793, "-3.141592653589793"},
	}

	for _, pair := range ioPairs {
		assert.Equal(t, pair.output, Float64ToString(pair.input), pair.input)
	}
}

func TestBooleanToBit(t *testing.T) {
	assert.Equal(t, 1, BooleanToBit(true))
	assert.Equal(t, 0, BooleanToBit(false))
}

func TestToString(t *testing.T) {
	{
		// Nil value
		_, err := ToString(nil, typing.KindDetails{})
		assert.ErrorContains(t, err, "colVal is nil")
	}
	{
		// Date
		{
			// time.Time
			value, err := ToString(time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC), typing.Date)
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01", value)
		}
		{
			// String
			value, err := ToString("2021-01-01", typing.Date)
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01", value)
		}
	}
	{
		// Time
		{
			// Valid
			{
				// String
				val, err := ToString("2021-01-01T03:52:00Z", typing.Time)
				assert.NoError(t, err)
				assert.Equal(t, "03:52:00", val)
			}
			{
				// time.Time
				actualValue, err := ToString(time.Date(2019, time.December, 31, 9, 27, 22, 0, time.UTC), typing.Time)
				assert.NoError(t, err)
				assert.Equal(t, "09:27:22", actualValue)
			}
		}
	}
	{
		// Timestamp NTZ
		{
			// time.Time
			value, err := ToString(time.Date(2021, time.January, 1, 17, 33, 4, 150_001_123, time.UTC), typing.TimestampNTZ)
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01T17:33:04.150001123", value)
		}
		{
			// String
			value, err := ToString("2021-01-01T17:33:04.150001123", typing.TimestampNTZ)
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2021, time.January, 1, 17, 33, 4, 150_001_123, time.UTC).Format(ext.RFC3339NoTZ), value)
		}
	}
	{
		// Timestamp TZ
		{
			// time.Time
			value, err := ToString(time.Date(2019, time.December, 31, 1, 2, 33, 400_999_991, time.UTC), typing.TimestampTZ)
			assert.NoError(t, err)
			assert.Equal(t, "2019-12-31T01:02:33.400999991Z", value)
		}
		{
			// String
			value, err := ToString("2019-12-31T01:02:33.400999991Z", typing.TimestampTZ)
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2019, time.December, 31, 1, 2, 33, 400_999_991, time.UTC).Format(time.RFC3339Nano), value)
		}
	}
	{
		// String
		// JSON
		val, err := ToString(map[string]any{"foo": "bar"}, typing.String)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		// Array
		val, err = ToString([]string{"foo", "bar"}, typing.String)
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)

		// Normal strings
		val, err = ToString("foo", typing.String)
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{
		// Struct
		val, err := ToString(map[string]any{"foo": "bar"}, typing.Struct)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		val, err = ToString(constants.ToastUnavailableValuePlaceholder, typing.Struct)
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, val)
	}
	{
		// Array
		{
			// Normal arrays
			val, err := ToString([]string{"foo", "bar"}, typing.Array)
			assert.NoError(t, err)
			assert.Equal(t, `["foo","bar"]`, val)
		}
		{
			// Toasted array
			val, err := ToString(constants.ToastUnavailableValuePlaceholder, typing.Array)
			assert.NoError(t, err)
			assert.Equal(t, `["__debezium_unavailable_value"]`, val)
		}
	}
	{
		// Integer column
		{
			// Float32 value
			val, err := ToString(float32(45452.999991), typing.Integer)
			assert.NoError(t, err)
			assert.Equal(t, "45453", val)
		}
		{
			// Float64 value
			val, err := ToString(45452.999991, typing.Integer)
			assert.NoError(t, err)
			assert.Equal(t, "45452.999991", val)
		}
		{
			// Integer value
			val, err := ToString(32, typing.Integer)
			assert.NoError(t, err)
			assert.Equal(t, "32", val)
		}
		{
			// Boolean values
			val, err := ToString(true, typing.Integer)
			assert.NoError(t, err)
			assert.Equal(t, "1", val)

			val, err = ToString(false, typing.Integer)
			assert.NoError(t, err)
			assert.Equal(t, "0", val)
		}
	}
	{
		// Extended Decimal
		{
			// Float32
			val, err := ToString(float32(123.45), typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
		{
			// Float64
			val, err := ToString(123.45, typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
		{
			// String
			val, err := ToString("123.45", typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "123.45", val)
		}
		{
			// Decimal
			val, err := ToString(decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("585692791691858.25"), 38), typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "585692791691858.25", val)
		}
		{
			// Int32
			val, err := ToString(int32(123), typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "123", val)
		}
		{
			// Int64
			val, err := ToString(int64(123), typing.EDecimal)
			assert.NoError(t, err)
			assert.Equal(t, "123", val)
		}
	}
}
