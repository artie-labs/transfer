package mssql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func TestParseValue(t *testing.T) {
	{
		val, err := parseValue(nil, columns.Column{})
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		// Date
		{
			// String
			val, err := parseValue("2021-01-01", columns.NewColumn("date", typing.Date))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01", val.(time.Time).Format(ext.PostgresDateFormat))
		}
		{
			// time.Time
			val, err := parseValue(time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC), columns.NewColumn("date", typing.Date))
			assert.NoError(t, err)
			assert.Equal(t, "2021-01-01", val.(time.Time).Format(ext.PostgresDateFormat))
		}
	}
	{
		// Timestamp NTZ
		{
			// String
			val, err := parseValue("2021-01-04T09:32:00", columns.NewColumn("timestamp_ntz", typing.TimestampNTZ))
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), val.(time.Time))
		}
		{
			// time.Time
			val, err := parseValue(time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), columns.NewColumn("timestamp_ntz", typing.TimestampNTZ))
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), val.(time.Time))
		}
	}
	{
		// Timestamp TZ
		{
			// String
			val, err := parseValue("2021-01-04T09:32:00Z", columns.NewColumn("timestamp_tz", typing.TimestampTZ))
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), val.(time.Time))
		}
		{
			// time.Time
			val, err := parseValue(time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), columns.NewColumn("timestamp_tz", typing.TimestampTZ))
			assert.NoError(t, err)
			assert.Equal(t, time.Date(2021, time.January, 4, 9, 32, 0, 0, time.UTC), val.(time.Time))
		}
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
		stringCol.KindDetails.OptionalStringPrecision = typing.ToPtr(int32(25))

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
}
