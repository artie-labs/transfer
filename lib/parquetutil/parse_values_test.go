package parquetutil

import (
	"testing"

	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	{
		// Nil
		value, err := ParseValue(nil, columns.Column{})
		assert.NoError(t, err)
		assert.Nil(t, value)
	}
	{
		// String
		value, err := ParseValue("test", columns.NewColumn("", typing.String))
		assert.NoError(t, err)
		assert.Equal(t, "test", value)
	}
	{
		// Struct
		value, err := ParseValue(map[string]any{"foo": "bar"}, columns.NewColumn("", typing.Struct))
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, value)
	}
	{
		// Arrays
		{
			// Arrays (numbers - converted to string)
			value, err := ParseValue([]any{123, 456}, columns.NewColumn("", typing.Array))
			assert.NoError(t, err)
			assert.Equal(t, []string{"123", "456"}, value)
		}
		{
			// Arrays (booleans - converted to string)
			value, err := ParseValue([]any{false, true, false}, columns.NewColumn("", typing.Array))
			assert.NoError(t, err)
			assert.Equal(t, []string{"false", "true", "false"}, value)
		}
	}
	{
		// Decimal
		value, err := ParseValue(decimal.NewDecimalWithPrecision(
			numbers.MustParseDecimal("5000.22320"), 30),
			columns.NewColumn("", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 5))),
		)

		assert.NoError(t, err)
		assert.Equal(t, "5000.22320", value)
	}
	{
		// Time
		eTime := typing.ETime
		eTime.ExtendedTimeDetails = &ext.Time
		value, err := ParseValue("03:15:00", columns.NewColumn("", eTime))
		assert.NoError(t, err)
		assert.Equal(t, "03:15:00+00", value)
	}
	{
		// Date
		eDate := typing.ETime
		eDate.ExtendedTimeDetails = &ext.Date
		value, err := ParseValue("2022-12-25", columns.NewColumn("", eDate))
		assert.NoError(t, err)
		assert.Equal(t, "2022-12-25", value)
	}
	{
		// Timestamp TZ
		eDateTime := typing.ETime
		eDateTime.ExtendedTimeDetails = &ext.TimestampTz
		value, err := ParseValue("2023-04-24T17:29:05.69944Z", columns.NewColumn("", eDateTime))
		assert.NoError(t, err)
		assert.Equal(t, int64(1682357345699), value)
	}
}
