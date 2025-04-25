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
		assert.Equal(t, "5000.22320", types.DECIMAL_BYTE_ARRAY_ToString([]byte(value.(string)), 30, 5))
	}
	{
		// Time
		value, err := ParseValue("03:15:00", typing.Time)
		assert.NoError(t, err)
		assert.Equal(t, int32(11700000), value)

		converted, err := converters.Time{}.Convert(int64(value.(int32)))
		assert.NoError(t, err)
		assert.Equal(t, "03:15:00", converted.(time.Time).Format(time.TimeOnly))
	}
	{
		// Date
		value, err := ParseValue("2022-12-25", typing.Date)
		assert.NoError(t, err)
		assert.Equal(t, int32(19351), value)
	}
	{
		// Timestamp TZ
		value, err := ParseValue("2023-04-24T17:29:05.69944Z", typing.TimestampTZ)
		assert.NoError(t, err)
		assert.Equal(t, int64(1682357345699), value)
	}
}

func Test_padBytesLeft(t *testing.T) {
	{
		// No padding needed
		result, err := padBytesLeft([]byte("123"), 3)
		assert.NoError(t, err)
		assert.Equal(t, []byte("123"), result)
	}
	{
		// Pad with zeros
		result, err := padBytesLeft([]byte("123"), 5)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0, 0, '1', '2', '3'}, result)
	}
	{
		// Empty input
		result, err := padBytesLeft([]byte{}, 3)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0, 0, 0}, result)
	}
	{
		// Single byte
		result, err := padBytesLeft([]byte{1}, 3)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0, 0, 1}, result)
	}
	{
		// Input longer than target length
		result, err := padBytesLeft([]byte("12345"), 3)
		assert.Error(t, err)
		assert.Equal(t, "bytes (5) are longer than the length: 3", err.Error())
		assert.Nil(t, result)
	}
	{
		// Zero length
		result, err := padBytesLeft([]byte{}, 0)
		assert.NoError(t, err)
		assert.Equal(t, []byte{}, result)
	}
}
