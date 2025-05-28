package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestDatabricksDialect_DataTypeForKind(t *testing.T) {
	{
		// Float
		assert.Equal(t, "DOUBLE", DatabricksDialect{}.DataTypeForKind(typing.Float, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// Integer
		assert.Equal(t, "BIGINT", DatabricksDialect{}.DataTypeForKind(typing.Integer, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// Variant
		assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Struct.Kind}, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// Array
		assert.Equal(t, "ARRAY<string>", DatabricksDialect{}.DataTypeForKind(typing.Array, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// String
		assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// Boolean
		assert.Equal(t, "BOOLEAN", DatabricksDialect{}.DataTypeForKind(typing.Boolean, false, config.SharedDestinationColumnSettings{}))
	}
	{
		// Times
		{
			// Date
			assert.Equal(t, "DATE", DatabricksDialect{}.DataTypeForKind(typing.Date, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// Timestamp
			assert.Equal(t, "TIMESTAMP", DatabricksDialect{}.DataTypeForKind(typing.TimestampTZ, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// Timestamp (w/o timezone)
			assert.Equal(t, "TIMESTAMP_NTZ", DatabricksDialect{}.DataTypeForKind(typing.TimestampNTZ, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// Time
			assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.Time, false, config.SharedDestinationColumnSettings{}))
		}
	}
	{
		// Decimals
		{
			// Below 38 precision
			assert.Equal(t, "DECIMAL(10, 2)", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(10, 2))}, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// Above 38 precision
			assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(40, 2))}, false, config.SharedDestinationColumnSettings{}))
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
