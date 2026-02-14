package primitives

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt64Converter_Convert(t *testing.T) {
	converter := Int64Converter{}
	{
		// Test converting valid string to int64
		got, err := converter.Convert("123")
		assert.NoError(t, err)
		assert.Equal(t, int64(123), got)
	}
	{
		// Test error handling for invalid string
		got, err := converter.Convert("not a number")
		assert.Error(t, err)
		assert.Equal(t, int64(0), got)
	}
	{
		// Test converting int16 to int64
		got, err := converter.Convert(int16(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), got)
	}
	{
		// Test converting int32 to int64
		got, err := converter.Convert(int32(456))
		assert.NoError(t, err)
		assert.Equal(t, int64(456), got)
	}
	{
		// Test converting int to int64
		got, err := converter.Convert(789)
		assert.NoError(t, err)
		assert.Equal(t, int64(789), got)
	}
	{
		// Test converting int64 to int64
		got, err := converter.Convert(int64(101112))
		assert.NoError(t, err)
		assert.Equal(t, int64(101112), got)
	}
	{
		// Test error handling for unsupported type
		got, err := converter.Convert(float64(123.45))
		assert.Error(t, err)
		assert.Equal(t, int64(0), got)
	}
	{
		// Floats
		{
			// float64 - valid whole number
			value, err := converter.Convert(float64(1234))
			assert.NoError(t, err)
			assert.Equal(t, int64(1234), value)
		}
		{
			// float64 - negative valid whole number
			value, err := converter.Convert(float64(-1234))
			assert.NoError(t, err)
			assert.Equal(t, int64(-1234), value)
		}
		{
			// float64 - zero
			value, err := converter.Convert(float64(0))
			assert.NoError(t, err)
			assert.Equal(t, int64(0), value)
		}
		{
			// float64 - large valid whole number that can be exactly represented
			value, err := converter.Convert(float64(1 << 50)) // 2^50 = 1125899906842624, exactly representable as float64
			assert.NoError(t, err)
			assert.Equal(t, int64(1<<50), value)
		}
		{
			// float64 - has fractional component
			_, err := converter.Convert(float64(1234.5))
			assert.ErrorContains(t, err, "float64 (1234.500000) has fractional component")
		}
		{
			// float64 - small fractional component
			_, err := converter.Convert(float64(1234.1))
			assert.ErrorContains(t, err, "float64 (1234.100000) has fractional component")
		}
		{
			// float64 - negative fractional component
			_, err := converter.Convert(float64(-1234.5))
			assert.ErrorContains(t, err, "float64 (-1234.500000) has fractional component")
		}
		{
			// float64 - positive overflow
			_, err := converter.Convert(float64(math.MaxInt64) * 2)
			assert.ErrorContains(t, err, "overflows int64")
		}
		{
			// float64 - negative overflow
			_, err := converter.Convert(float64(math.MinInt64) * 2)
			assert.ErrorContains(t, err, "overflows int64")
		}
		{
			// float64 - positive infinity
			_, err := converter.Convert(math.Inf(1))
			assert.ErrorContains(t, err, "overflows int64")
		}
		{
			// float64 - negative infinity
			_, err := converter.Convert(math.Inf(-1))
			assert.ErrorContains(t, err, "overflows int64")
		}
		{
			// float64 - NaN
			_, err := converter.Convert(math.NaN())
			assert.ErrorContains(t, err, "has fractional component")
		}
	}
	{
		// json.Number - valid integer
		got, err := converter.Convert(json.Number("42"))
		assert.NoError(t, err)
		assert.Equal(t, int64(42), got)
	}
	{
		// json.Number - large int64
		got, err := converter.Convert(json.Number("9223372036854775806"))
		assert.NoError(t, err)
		assert.Equal(t, int64(9223372036854775806), got)
	}
	{
		// json.Number - invalid (float with fraction)
		_, err := converter.Convert(json.Number("42.5"))
		assert.Error(t, err)
	}
}

func TestBooleanConverter_Convert(t *testing.T) {
	converter := BooleanConverter{}

	trueVariants := []any{"true", true}
	for _, variant := range trueVariants {
		got, err := converter.Convert(variant)
		assert.NoError(t, err)
		assert.True(t, got)
	}

	falseVariants := []any{"false", false}
	for _, variant := range falseVariants {
		got, err := converter.Convert(variant)
		assert.NoError(t, err)
		assert.False(t, got)
	}
}

func TestFloat32Converter_Convert(t *testing.T) {
	converter := Float32Converter{}
	{
		// Float32
		actual, err := converter.Convert(float32(1.1))
		assert.NoError(t, err)
		assert.Equal(t, float32(1.1), actual)
	}
	{
		// Float64
		{
			// Max float
			_, err := converter.Convert(math.MaxFloat64)
			assert.ErrorContains(t, err, "value overflows float32")
		}
		{
			// Min float
			_, err := converter.Convert(-math.MaxFloat64)
			assert.ErrorContains(t, err, "value overflows float32")
		}
		{
			actual, err := converter.Convert(float64(123.55))
			assert.NoError(t, err)
			assert.Equal(t, float32(123.55), actual)
		}
	}
	{
		// String
		actual, err := converter.Convert("1.1")
		assert.NoError(t, err)
		assert.Equal(t, float32(1.1), actual)
	}
	{
		// json.Number
		actual, err := converter.Convert(json.Number("1.1"))
		assert.NoError(t, err)
		assert.Equal(t, float32(1.1), actual)
	}
	{
		// json.Number - integer
		actual, err := converter.Convert(json.Number("42"))
		assert.NoError(t, err)
		assert.Equal(t, float32(42), actual)
	}
	{
		// Irrelevant
		_, err := converter.Convert(true)
		assert.ErrorContains(t, err, "failed to parse float32, unsupported type: bool")
	}
}
