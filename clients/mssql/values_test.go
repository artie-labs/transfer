package mssql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestParseValue(t *testing.T) {
	{
		val, err := parseValue(nil, columns.Column{}, nil)
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
	{
		val, err := parseValue("string value", columns.NewColumn("foo", typing.String), nil)
		assert.NoError(t, err)
		assert.Equal(t, "string value", val)

		// We don't need to escape backslashes.
		val, err = parseValue(`dusty o\donald`, columns.NewColumn("foo", typing.String), nil)
		assert.NoError(t, err)
		assert.Equal(t, `dusty o\donald`, val)

		// If the string precision exceeds the value, we'll need to insert an exceeded value.
		stringCol := columns.NewColumn("foo", typing.String)
		stringCol.KindDetails.OptionalStringPrecision = ptr.ToInt(25)

		val, err = parseValue(`abcdefabcdefabcdefabcdef113321`, stringCol, nil)
		assert.NoError(t, err)
		assert.Equal(t, constants.ExceededValueMarker, val)
	}
	{
		val, err := parseValue(map[string]any{"foo": "bar"}, columns.NewColumn("json", typing.Struct), nil)
		assert.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, val)
	}
	{
		val, err := parseValue([]any{"foo", "bar"}, columns.NewColumn("array", typing.Array), nil)
		assert.NoError(t, err)
		assert.Equal(t, `["foo","bar"]`, val)
	}
	{
		// Integers
		val, err := parseValue(1234, columns.NewColumn("int", typing.Integer), nil)
		assert.NoError(t, err)
		assert.Equal(t, 1234, val)

		// Should be able to handle string ints
		val, err = parseValue("1234", columns.NewColumn("float", typing.Integer), nil)
		assert.NoError(t, err)
		assert.Equal(t, 1234, val)
	}
	{
		// Floats
		val, err := parseValue(1234.5678, columns.NewColumn("float", typing.Float), nil)
		assert.NoError(t, err)
		assert.Equal(t, 1234.5678, val)

		// Should be able to handle string floats
		val, err = parseValue("1234.5678", columns.NewColumn("float", typing.Float), nil)
		assert.NoError(t, err)
		assert.Equal(t, 1234.5678, val)
	}
	{
		// Boolean, but the column is an integer column.
		val, err := parseValue(true, columns.NewColumn("bigint", typing.Integer), nil)
		assert.NoError(t, err)
		assert.Equal(t, 1, val)

		// Booleans
		val, err = parseValue(true, columns.NewColumn("bool", typing.Boolean), nil)
		assert.NoError(t, err)
		assert.True(t, val.(bool))

		val, err = parseValue(false, columns.NewColumn("bool", typing.Boolean), nil)
		assert.NoError(t, err)
		assert.False(t, val.(bool))

		// Should be able to handle string booleans
		val, err = parseValue("true", columns.NewColumn("bool", typing.Boolean), nil)
		assert.NoError(t, err)
		assert.True(t, val.(bool))

		val, err = parseValue("false", columns.NewColumn("bool", typing.Boolean), nil)
		assert.NoError(t, err)
		assert.False(t, val.(bool))
	}
}
