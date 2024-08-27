package converters

import (
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/stretchr/testify/assert"
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
}

func TestIntegerConverter_Convert(t *testing.T) {
	converter := IntegerConverter{}
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
