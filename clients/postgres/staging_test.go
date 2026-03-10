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

func TestParseValue_Interval(t *testing.T) {
	col := columns.NewColumn("duration", typing.Interval)

	{
		// nil value
		result, err := parseValue(nil, col)
		assert.NoError(t, err)
		assert.Nil(t, result)
	}
	{
		// Valid interval string
		result, err := parseValue("P1Y2M3DT4H5M6.7S", col)
		assert.NoError(t, err)
		assert.Equal(t, "P1Y2M3DT4H5M6.7S", result)
	}
	{
		// Wrong type (not a string)
		_, err := parseValue(12345, col)
		assert.ErrorContains(t, err, "expected type string")
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
