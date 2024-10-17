package dialect

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestDatabricksDialect_DataTypeForKind(t *testing.T) {
	{
		// Float
		assert.Equal(t, "DOUBLE", DatabricksDialect{}.DataTypeForKind(typing.Float, false))
	}
	{
		// Integer
		assert.Equal(t, "BIGINT", DatabricksDialect{}.DataTypeForKind(typing.Integer, false))
	}
	{
		// Variant
		assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Struct.Kind}, false))
	}
	{
		// Array
		assert.Equal(t, "ARRAY<string>", DatabricksDialect{}.DataTypeForKind(typing.Array, false))
	}
	{
		// String
		assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.String, false))
	}
	{
		// Boolean
		assert.Equal(t, "BOOLEAN", DatabricksDialect{}.DataTypeForKind(typing.Boolean, false))
	}
	{
		// Times
		{
			// Date
			assert.Equal(t, "DATE", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.ETime.Kind, ExtendedTimeDetails: &ext.NestedKind{Type: ext.DateKindType}}, false))
		}
		{
			// Timestamp
			assert.Equal(t, "TIMESTAMP", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.ETime.Kind, ExtendedTimeDetails: &ext.NestedKind{Type: ext.TimestampTzKindType}}, false))
		}
		{
			// Timestamp (w/o timezone)
			assert.Equal(t, "TIMESTAMP", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.ETime.Kind, ExtendedTimeDetails: &ext.NestedKind{Type: ext.TimestampTzKindType}}, false))
		}
		{
			// Time
			assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.ETime.Kind, ExtendedTimeDetails: &ext.NestedKind{Type: ext.TimeKindType}}, false))
		}
	}
	{
		// Decimals
		{
			// Below 38 precision
			assert.Equal(t, "DECIMAL(10, 2)", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(10, 2))}, false))
		}
		{
			// Above 38 precision
			assert.Equal(t, "STRING", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(40, 2))}, false))
		}
	}
}

func TestDatabricksDialect_KindForDataType(t *testing.T) {
	{
		// Decimal
		{
			// Invalid
			_, err := DatabricksDialect{}.KindForDataType("DECIMAL(9", "")
			assert.ErrorContains(t, err, "missing closing parenthesis")
		}
		{
			// Valid
			kd, err := DatabricksDialect{}.KindForDataType("DECIMAL(10, 2)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.EDecimal.Kind, ExtendedDecimalDetails: typing.ToPtr(decimal.NewDetails(10, 2))}, kd)
		}
	}
	{
		// Array
		kd, err := DatabricksDialect{}.KindForDataType("ARRAY<string>", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kd)
	}
	{
		// String
		kd, err := DatabricksDialect{}.KindForDataType("STRING", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// Binary
		kd, err := DatabricksDialect{}.KindForDataType("BINARY", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.String, kd)
	}
	{
		// BigInt
		kd, err := DatabricksDialect{}.KindForDataType("BIGINT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, kd)
	}
	{
		// Boolean
		kd, err := DatabricksDialect{}.KindForDataType("BOOLEAN", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Date
		kd, err := DatabricksDialect{}.KindForDataType("DATE", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.ETime.Kind, ExtendedTimeDetails: &ext.NestedKind{Type: ext.DateKindType}}, kd)
	}
	{
		// Double
		kd, err := DatabricksDialect{}.KindForDataType("DOUBLE", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kd)
	}
	{
		// Float
		kd, err := DatabricksDialect{}.KindForDataType("FLOAT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Float, kd)
	}
	{
		// Integer
		kd, err := DatabricksDialect{}.KindForDataType("INT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
	}
	{
		// Small Int
		kd, err := DatabricksDialect{}.KindForDataType("SMALLINT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, kd)
	}
	{
		// Timestamp
		kd, err := DatabricksDialect{}.KindForDataType("TIMESTAMP", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType), kd)
	}
	{
		// Timestamp NTZ
		kd, err := DatabricksDialect{}.KindForDataType("TIMESTAMP_NTZ", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimestampNTZKindType), kd)
	}
	{
		// Variant
		kd, err := DatabricksDialect{}.KindForDataType("VARIANT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind}, kd)
	}
	{
		// Object
		kd, err := DatabricksDialect{}.KindForDataType("OBJECT", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind}, kd)
	}
}
