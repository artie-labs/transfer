package postgres

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestParseValue_String(t *testing.T) {
	col := columns.NewColumn("name", typing.String)

	{
		// nil value
		result, err := parseValue(nil, col)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
	{
		// String value
		result, err := parseValue("hello", col)
		assert.NoError(t, err)
		assert.Equal(t, "hello", result)
	}
	{
		// int64 value - should be converted to string, not error
		result, err := parseValue(int64(12345), col)
		assert.NoError(t, err)
		assert.Equal(t, "12345", result)
	}
	{
		// int value
		result, err := parseValue(42, col)
		assert.NoError(t, err)
		assert.Equal(t, "42", result)
	}
	{
		// String with backslashes must not be doubled
		result, err := parseValue(`hello\world`, col)
		assert.NoError(t, err)
		assert.Equal(t, `hello\world`, result)
	}
}

func TestParseValue_Array(t *testing.T) {
	{
		// nil value
		col := columns.NewColumn("tags", typing.Array)
		result, err := parseValue(nil, col)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
	{
		// OptionalArrayKind not set - should cast to text ([]string)
		col := columns.NewColumn("tags", typing.Array)
		result, err := parseValue([]any{"a", "b", "c"}, col)
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, result)
	}
	{
		// OptionalArrayKind is string - should cast to text ([]string)
		col := columns.NewColumn("tags", typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.String})
		result, err := parseValue([]any{"hello", "world"}, col)
		assert.NoError(t, err)
		assert.Equal(t, []string{"hello", "world"}, result)
	}
	{
		// OptionalArrayKind is integer - should return value as-is
		intKind := typing.Integer
		col := columns.NewColumn("ids", typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &intKind})
		result, err := parseValue([]any{1, 2, 3}, col)
		assert.NoError(t, err)
		assert.Equal(t, []any{1, 2, 3}, result)
	}
	{
		// OptionalArrayKind is boolean - should return value as-is
		col := columns.NewColumn("flags", typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.Boolean})
		result, err := parseValue([]any{true, false}, col)
		assert.NoError(t, err)
		assert.Equal(t, []any{true, false}, result)
	}
	{
		// OptionalArrayKind is float - should return value as-is
		col := columns.NewColumn("scores", typing.KindDetails{Kind: typing.Array.Kind, OptionalArrayKind: &typing.Float})
		result, err := parseValue([]any{1.1, 2.2}, col)
		assert.NoError(t, err)
		assert.Equal(t, []any{1.1, 2.2}, result)
	}
	{
		// Non-string elements with text array should be converted to strings
		col := columns.NewColumn("mixed", typing.Array)
		result, err := parseValue([]any{42, true, 3.14}, col)
		assert.NoError(t, err)
		assert.Equal(t, []string{"42", "true", "3.14"}, result)
	}
	{
		// Single non-slice value with text array should be wrapped and converted
		col := columns.NewColumn("single", typing.Array)
		result, err := parseValue("solo", col)
		assert.NoError(t, err)
		assert.Equal(t, []string{"solo"}, result)
	}
}

func TestParseValue_Bytes(t *testing.T) {
	col := columns.NewColumn("data", typing.Bytes)

	{
		// nil value
		result, err := parseValue(nil, col)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
	{
		// Valid base64 string
		rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		encoded := base64.StdEncoding.EncodeToString(rawBytes)
		result, err := parseValue(encoded, col)
		assert.NoError(t, err)
		assert.Equal(t, rawBytes, result)
	}
	{
		// TOAST placeholder should be returned as []byte so pgx can encode it as bytea
		result, err := parseValue(constants.ToastUnavailableValuePlaceholder, col)
		assert.NoError(t, err)
		assert.Equal(t, []byte(constants.ToastUnavailableValuePlaceholder), result)
	}
	{
		// Invalid base64 string
		_, err := parseValue("not-valid-base64!!", col)
		assert.ErrorContains(t, err, "illegal base64")
	}
	{
		// Wrong type (not a string)
		_, err := parseValue(12345, col)
		assert.ErrorContains(t, err, "expected type string")
	}
}
