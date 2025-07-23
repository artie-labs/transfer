package dialect

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/stretchr/testify/assert"
)

func TestKindForDataType(t *testing.T) {
	expectedTypeToKindMap := map[string]typing.KindDetails{
		// Numbers:
		"numeric(5, 2)": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)),
		"numeric(5, 0)": typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
		"numeric(5)":    typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 0)),
		// Variable numeric type:
		"numeric":          typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)),
		"float":            typing.Float,
		"real":             typing.Float,
		"double":           typing.Float,
		"double precision": typing.Float,
		"smallint":         typing.BuildIntegerKind(typing.SmallIntegerKind),
		"integer":          typing.BuildIntegerKind(typing.IntegerKind),
		"bigint":           typing.BuildIntegerKind(typing.BigIntegerKind),
		// String data types:
		"character varying(5)": {Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(5))},
		"character(5)":         {Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(5))},
		"text":                 typing.String,
		// Boolean:
		"boolean": typing.Boolean,
		// Date and timestamp data types:
		"date":                            typing.Date,
		"time":                            typing.Time,
		"timestamp with time zone":        typing.TimestampTZ,
		"timestamp (5) with time zone":    typing.TimestampTZ,
		"timestamp without time zone":     typing.TimestampNTZ,
		"timestamp (4) without time zone": typing.TimestampNTZ,
		// Other data types:
		"json": typing.Struct,
	}

	for dataType, expectedKind := range expectedTypeToKindMap {
		kind, err := PostgresDialect{}.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, expectedKind, kind)
	}
}
func TestStripPrecision(t *testing.T) {
	{
		// Test with a string that has a precision
		s := "timestamp(6)"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp", stripped)
		assert.Equal(t, "6", metadata)
	}
	{
		// Test with a string that doesn't have a precision
		s := "timestamp"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, s, stripped)
		assert.Empty(t, metadata)
	}
	{
		// Test with a string that has a precision but no parentheses
		s := "timestamp 6"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp 6", stripped)
		assert.Empty(t, metadata)
	}
	{
		// Test with multiple precisions, it will only strip the first precision
		s := "timestamp(6) timestamp(3)"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, "timestamp timestamp(3)", stripped)
		assert.Equal(t, "6", metadata)
	}
	{
		s := "timestamp(6"
		stripped, metadata := StripPrecision(s)
		assert.Equal(t, s, stripped)
		assert.Empty(t, metadata)
	}
}
