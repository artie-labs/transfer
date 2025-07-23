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
	}

	for dataType, expectedKind := range expectedTypeToKindMap {
		kind, err := PostgresDialect{}.KindForDataType(dataType)
		assert.NoError(t, err)
		assert.Equal(t, expectedKind, kind)
	}
}
