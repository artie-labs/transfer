package parquetutil

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestParseValueForArrow(t *testing.T) {
	{
		// Nil
		value, err := ParseValueForArrow(nil, typing.KindDetails{}, nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// String
		value, err := ParseValueForArrow("test", typing.String, nil)
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Struct
		value, err := ParseValueForArrow(map[string]any{"foo": "bar"}, typing.Struct, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, value)
	}
	{
		// Arrays
		{
			// Arrays (numbers - converted to string)
			value, err := ParseValueForArrow([]any{123, 456}, typing.Array, nil)
			assert.NoError(t, err)
			assert.Equal(t, []string{"123", "456"}, value)
		}
		{
			// Arrays (booleans - converted to string)
			value, err := ParseValueForArrow([]any{false, true, false}, typing.Array, nil)
			assert.NoError(t, err)
			assert.Equal(t, []string{"false", "true", "false"}, value)
		}
	}
	{
		// Decimal with precision
		value, err := ParseValueForArrow(decimal.NewDecimalWithPrecision(
			numbers.MustParseDecimal("5000.22320"), 30),
			typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 5)),
			nil,
		)

		assert.NoError(t, err)
		// For arrow, we should get a decimal128.Num for valid precision
		dec128, ok := value.(decimal128.Num)
		assert.True(t, ok)
		assert.NotNil(t, dec128)
	}
	{
		// Decimal without precision - should fallback to string
		value, err := ParseValueForArrow(decimal.NewDecimalWithPrecision(
			numbers.MustParseDecimal("5000.22320"), 30),
			typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, 5)),
			nil,
		)

		assert.NoError(t, err)
		assert.Equal(t, "5000.22320", value)
	}
	{
		// Time
		value, err := ParseValueForArrow("03:15:00", typing.Time, nil)
		assert.NoError(t, err)
		assert.Equal(t, int32(11700000), value)
	}
	{
		// Date
		value, err := ParseValueForArrow("2022-12-25", typing.Date, nil)
		assert.NoError(t, err)
		assert.Equal(t, int32(19351), value)
	}
	{
		// TIMESTAMP NTZ
		_time := time.Date(2023, 4, 24, 17, 29, 5, 699_000_000, time.UTC)
		{
			// No location
			value, err := ParseValueForArrow(_time, typing.TimestampNTZ, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1_682_357_345_699), value)
		}
		{
			// With location
			est, err := time.LoadLocation("America/New_York")
			assert.NoError(t, err)

			value, err := ParseValueForArrow(_time, typing.TimestampNTZ, est)
			assert.NoError(t, err)
			assert.Equal(t, int64(1_682_342_945_699), value)

			_, offset := _time.In(est).Zone()
			// This needs to be subtract since we need to do the opposite of what we're doing in [ParseValueForArrow] to unravel the value back to UTC.
			estTime := time.UnixMilli(value.(int64) - int64(offset*1000)).In(time.UTC)
			assert.Equal(t, _time, estTime)
		}
	}
	{
		// Timestamp TZ
		_time := time.Date(2023, 4, 24, 17, 29, 5, 699_000_000, time.UTC)
		{
			// No location
			value, err := ParseValueForArrow(_time, typing.TimestampTZ, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1_682_357_345_699), value)
		}
		{
			// With location
			est, err := time.LoadLocation("America/New_York")
			assert.NoError(t, err)

			value, err := ParseValueForArrow(_time, typing.TimestampTZ, est)
			assert.NoError(t, err)
			assert.Equal(t, int64(1_682_342_945_699), value)

			_, offset := _time.In(est).Zone()
			estTime := time.UnixMilli(value.(int64) - int64(offset*1000)).In(time.UTC)
			assert.Equal(t, _time, estTime)
		}
	}
	{
		// Boolean
		value, err := ParseValueForArrow(true, typing.Boolean, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, value)
	}
	{
		// Float
		value, err := ParseValueForArrow(3.14, typing.Float, nil)
		assert.NoError(t, err)
		assert.Equal(t, 3.14, value)
	}
}
