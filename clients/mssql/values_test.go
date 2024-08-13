package mssql

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	{
		val, err := parseValue(nil, columns.Column{})
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		val, err := parseValue("string value", columns.NewColumn("foo", typing.String))
		assert.NoError(t, err)
		assert.Equal(t, "string value", val)

		// We don't need to escape backslashes.
		val, err = parseValue(`dusty o\donald`, columns.NewColumn("foo", typing.String))
		assert.NoError(t, err)
		assert.Equal(t, `dusty o\donald`, val)

		// If the string precision exceeds the value, we'll need to insert an exceeded value.
		stringCol := columns.NewColumn("foo", typing.String)
		stringCol.KindDetails.OptionalStringPrecision = ptr.ToInt32(25)

		val, err = parseValue(`abcdefabcdefabcdefabcdef113321`, stringCol)
		assert.NoError(t, err)
		assert.Equal(t, constants.ExceededValueMarker, val)
	}
	{
		val, err := parseValue(map[string]any{"foo": "bar"}, columns.NewColumn("json", typing.Struct))
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)
	}
	{
		val, err := parseValue([]any{"foo", "bar"}, columns.NewColumn("array", typing.Array))
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)
	}
	{
		// Integers
		val, err := parseValue(1234, columns.NewColumn("int", typing.Integer))
		assert.NoError(t, err)
		assert.Equal(t, 1234, val)

		// Should be able to handle string ints
		val, err = parseValue("1234", columns.NewColumn("float", typing.Integer))
		assert.NoError(t, err)
		assert.Equal(t, 1234, val)
	}
	{
		// Floats
		val, err := parseValue(1234.5678, columns.NewColumn("float", typing.Float))
		assert.NoError(t, err)
		assert.Equal(t, 1234.5678, val)

		// Should be able to handle string floats
		val, err = parseValue("1234.5678", columns.NewColumn("float", typing.Float))
		assert.NoError(t, err)
		assert.Equal(t, 1234.5678, val)
	}
	{
		// Boolean, but the column is an integer column.
		val, err := parseValue(true, columns.NewColumn("bigint", typing.Integer))
		assert.NoError(t, err)
		assert.Equal(t, 1, val)

		// Booleans
		val, err = parseValue(true, columns.NewColumn("bool", typing.Boolean))
		assert.NoError(t, err)
		assert.True(t, val.(bool))

		val, err = parseValue(false, columns.NewColumn("bool", typing.Boolean))
		assert.NoError(t, err)
		assert.False(t, val.(bool))

		// Should be able to handle string booleans
		val, err = parseValue("true", columns.NewColumn("bool", typing.Boolean))
		assert.NoError(t, err)
		assert.True(t, val.(bool))

		val, err = parseValue("false", columns.NewColumn("bool", typing.Boolean))
		assert.NoError(t, err)
		assert.False(t, val.(bool))
	}
	{
		// Extended time
		{
			// String
			val, err := parseValue("2021-01-01T00:00:00Z", columns.NewColumn("time", typing.ETime))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01T00:00:00Z", val)
		}
		{
			// Extended time object
			ts := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

			val, err := parseValue(
				ext.NewExtendedTime(ts, ext.DateKindType, ext.ISO8601),
				columns.NewColumn("time", typing.ETime),
			)
			assert.NoError(t, err)
			assert.Equal(t, ts, val)
		}
		{
			// Wrong data type
			val, err := parseValue(123, columns.NewColumn("time", typing.ETime))
			assert.ErrorContains(t, err, "expected colVal to be either string or *ext.ExtendedTime, type is: int")
			assert.Nil(t, val)
		}

	}
}
