package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestDatabricksDialect_DataTypeForKind(t *testing.T) {
	kindDetailsToExpectedMap := map[typing.KindDetails]string{
		typing.Float:        "DOUBLE",
		typing.Integer:      "BIGINT",
		typing.Struct:       "STRING",
		typing.Array:        "ARRAY<string>",
		typing.String:       "STRING",
		typing.Boolean:      "BOOLEAN",
		typing.Date:         "DATE",
		typing.TimestampTZ:  "TIMESTAMP",
		typing.TimestampNTZ: "TIMESTAMP_NTZ",
		typing.Time:         "STRING",
		typing.EDecimal:     "DECIMAL(10, 2)",
	}

	for kind, expected := range kindDetailsToExpectedMap {
		actual, err := DatabricksDialect{}.DataTypeForKind(kind, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
	{
		// Decimals
		{
			// Below 38 precision
			actual, err := DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(10, 2))}, false, config.SharedDestinationColumnSettings{})
			assert.NoError(t, err)
			assert.Equal(t, "DECIMAL(10, 2)", actual)
		}
		{
			// Above 38 precision
			actual, err := DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(40, 2))}, false, config.SharedDestinationColumnSettings{})
			assert.NoError(t, err)
			assert.Equal(t, "STRING", actual)
		}
	}
}

func TestDatabricksDialect_KindForDataType(t *testing.T) {
	{
		// Decimal
		{
			// Invalid
			_, err := DatabricksDialect{}.KindForDataType("DECIMAL(9")
			assert.ErrorContains(t, err, "missing closing parenthesis")
		}
		{
			// Valid
			kd, err := DatabricksDialect{}.KindForDataType("DECIMAL(10, 2)")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(10, 2))}, kd)
		}
	}
	{
		// Array
		kd, err := DatabricksDialect{}.KindForDataType("ARRAY<string>")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kd)
	}
	{
		// String
		kd, err := DatabricksDialect{}.KindForDataType("STRING")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// Binary
		kd, err := DatabricksDialect{}.KindForDataType("BINARY")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// BigInt
		kd, err := DatabricksDialect{}.KindForDataType("BIGINT")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, kd)
	}
	{
		// Boolean
		kd, err := DatabricksDialect{}.KindForDataType("BOOLEAN")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Date
		kd, err := DatabricksDialect{}.KindForDataType("DATE")
		assert.NoError(t, err)
		assert.Equal(t, typing.Date, kd)
	}
	{
		// Double
		kd, err := DatabricksDialect{}.KindForDataType("DOUBLE")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kd)
	}
	{
		// Float
		kd, err := DatabricksDialect{}.KindForDataType("FLOAT")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kd)
	}
	{
		// Integer
		kd, err := DatabricksDialect{}.KindForDataType("INT")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
	}
	{
		// Small Int
		kd, err := DatabricksDialect{}.KindForDataType("SMALLINT")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, kd)
	}
	{
		// Timestamp
		kd, err := DatabricksDialect{}.KindForDataType("TIMESTAMP")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kd)
	}
	{
		// Timestamp NTZ
		kd, err := DatabricksDialect{}.KindForDataType("TIMESTAMP_NTZ")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kd)
	}
	{
		// Variant
		kd, err := DatabricksDialect{}.KindForDataType("VARIANT")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind}, kd)
	}
	{
		// Object
		kd, err := DatabricksDialect{}.KindForDataType("OBJECT")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind}, kd)
	}
}
