package dialect

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
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
		assert.Equal(t, "VARIANT", DatabricksDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Struct.Kind}, false))
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
