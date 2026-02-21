package postgres

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

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
