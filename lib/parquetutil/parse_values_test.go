package parquetutil

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/types"
)

func TestParseValue(t *testing.T) {
	{
		// Nil
		value, err := ParseValue(nil, typing.KindDetails{}, nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// String
		value, err := ParseValue("test", typing.String, nil)
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Struct
		value, err := ParseValue(map[string]any{"foo": "bar"}, typing.Struct, nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, value)
	}
	{
		// Arrays
		{
			// Arrays (numbers - converted to string)
			value, err := ParseValue([]any{123, 456}, typing.Array, nil)
			assert.NoError(t, err)
			assert.Equal(t, []string{"123", "456"}, value)
		}
		{
			// Arrays (booleans - converted to string)
			value, err := ParseValue([]any{false, true, false}, typing.Array, nil)
			assert.NoError(t, err)
			assert.Equal(t, []string{"false", "true", "false"}, value)
		}
	}
	{
		// Decimal
		value, err := ParseValue(decimal.NewDecimalWithPrecision(
			numbers.MustParseDecimal("5000.22320"), 30),
			typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 5)),
			nil,
		)

		assert.NoError(t, err)
		assert.Equal(t, "5000.22320", types.DECIMAL_BYTE_ARRAY_ToString([]byte(value.(string)), 30, 5))
	}
	{
		// Time
		value, err := ParseValue("03:15:00", typing.Time, nil)
		assert.NoError(t, err)
		assert.Equal(t, int32(11700000), value)

		converted, err := converters.Time{}.Convert(int64(value.(int32)))
		assert.NoError(t, err)
		assert.Equal(t, "03:15:00", converted.(time.Time).Format(time.TimeOnly))
	}
	{
		// Date
		value, err := ParseValue("2022-12-25", typing.Date, nil)
		assert.NoError(t, err)
		assert.Equal(t, int32(19351), value)
	}
	{
		// TIMESTAMP NTZ
		{
			// No location
			value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampNTZ, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1682357345699), value)
		}
		{
			// With location
			est, err := time.LoadLocation("America/New_York")
			assert.NoError(t, err)

			value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampNTZ, est)
			assert.NoError(t, err)
			assert.Equal(t, int64(1682357345699), value)
		}
	}
	{
		// Timestamp TZ
		{
			// No location
			value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampTZ, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1682357345699), value)
		}
		{
			// With location
			est, err := time.LoadLocation("America/New_York")
			assert.NoError(t, err)

			value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampTZ, est)
			assert.NoError(t, err)
			assert.Equal(t, int64(1682357345699), value)
		}
	}
}
