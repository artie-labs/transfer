package parquetutil

import (
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	{
		// Nil
		value, err := ParseValue(nil, typing.KindDetails{})
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// String
		value, err := ParseValue("test", typing.String)
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Struct
		value, err := ParseValue(map[string]any{"foo": "bar"}, typing.Struct)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, value)
	}
	{
		// Arrays
		{
			// Arrays (numbers - converted to string)
			value, err := ParseValue([]any{123, 456}, typing.Array)
			assert.NoError(t, err)
			assert.Equal(t, []string{"123", "456"}, value)
		}
		{
			// Arrays (booleans - converted to string)
			value, err := ParseValue([]any{false, true, false}, typing.Array)
			assert.NoError(t, err)
			assert.Equal(t, []string{"false", "true", "false"}, value)
		}
	}
	{
		// Decimal
		value, err := ParseValue(decimal.NewDecimalWithPrecision(
			numbers.MustParseDecimal("5000.22320"), 30),
			typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 5)),
		)

		assert.NoError(t, err)
		assert.Equal(t, "5000.22320", value)
	}
	{
		// Time
		value, err := ParseValue("03:15:00", typing.Time)
		assert.NoError(t, err)
		assert.Equal(t, "03:15:00Z", value)
	}
	{
		// Date
		value, err := ParseValue("2022-12-25", typing.Date)
		assert.NoError(t, err)
		assert.Equal(t, "2022-12-25", value)
	}
	{
		// Timestamp TZ
		value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampTZ)
		assert.NoError(t, err)
		assert.Equal(t, int64(1682357345699), value)
	}
}
