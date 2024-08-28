package values

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestBooleanToBit(t *testing.T) {
	assert.Equal(t, 1, BooleanToBit(true))
	assert.Equal(t, 0, BooleanToBit(false))
}

func TestToString(t *testing.T) {
	{
		// Nil value
		_, err := ToString(nil, typing.KindDetails{}, nil)
		assert.ErrorContains(t, err, "colVal is nil")
	}
	{
		// ETime
		_, err := ToString("2021-01-01T00:00:00Z", typing.ETime, nil)
		assert.ErrorContains(t, err, "column kind details for extended time details is null")

		eTimeCol := columns.NewColumn("time", typing.ETime)
		eTimeCol.SourceKindDetails.ExtendedTimeDetails = &ext.NestedKind{Type: ext.TimeKindType}
		// Using `string`
		val, err := ToString("2021-01-01T03:52:00Z", eTimeCol.SourceKindDetails, nil)
		assert.NoError(t, err)
		assert.Equal(t, "03:52:00", val)

		// Using `*ExtendedTime`
		dustyBirthday := time.Date(2019, time.December, 31, 0, 0, 0, 0, time.UTC)
		originalFmt := "2006-01-02T15:04:05Z07:00"
		extendedTime := ext.NewExtendedTime(dustyBirthday, ext.DateTimeKindType, originalFmt)

		eTimeCol.SourceKindDetails.ExtendedTimeDetails = &ext.NestedKind{Type: ext.DateTimeKindType}
		actualValue, err := ToString(extendedTime, eTimeCol.SourceKindDetails, nil)
		assert.NoError(t, err)
		assert.Equal(t, extendedTime.String(originalFmt), actualValue)
	}
	{
		// String
		// JSON
		val, err := ToString(map[string]any{"foo": "bar"}, typing.String, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		// Array
		val, err = ToString([]string{"foo", "bar"}, typing.String, nil)
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)

		// Normal strings
		val, err = ToString("foo", typing.String, nil)
		assert.NoError(t, err)
		assert.Equal(t, "foo", val)
	}
	{
		// Struct
		val, err := ToString(map[string]any{"foo": "bar"}, typing.Struct, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)

		val, err = ToString(constants.ToastUnavailableValuePlaceholder, typing.Struct, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"key":"__debezium_unavailable_value"}`, val)
	}
	{
		// Array
		val, err := ToString([]string{"foo", "bar"}, typing.Array, nil)
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)
	}
	{
		// Integer
		// Floats first.
		val, err := ToString(float32(45452.999991), typing.Integer, nil)
		assert.NoError(t, err)
		assert.Equal(t, "45453", val)

		val, err = ToString(45452.999991, typing.Integer, nil)
		assert.NoError(t, err)
		assert.Equal(t, "45453", val)

		// Integer
		val, err = ToString(32, typing.Integer, nil)
		assert.NoError(t, err)
		assert.Equal(t, "32", val)

		// Booleans
		val, err = ToString(true, typing.Integer, nil)
		assert.NoError(t, err)
		assert.Equal(t, "1", val)

		val, err = ToString(false, typing.Integer, nil)
		assert.NoError(t, err)
		assert.Equal(t, "0", val)
	}
	{
		// Extended Decimal
		// Floats
		val, err := ToString(float32(123.45), typing.EDecimal, nil)
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)

		val, err = ToString(123.45, typing.EDecimal, nil)
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)

		// String
		val, err = ToString("123.45", typing.EDecimal, nil)
		assert.NoError(t, err)
		assert.Equal(t, "123.45", val)

		// Decimals
		value := decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("585692791691858.25"), 38)
		val, err = ToString(value, typing.EDecimal, nil)
		assert.NoError(t, err)
		assert.Equal(t, "585692791691858.25", val)
	}
}
