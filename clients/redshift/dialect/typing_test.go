package dialect

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestRedshiftDialect_DataTypeForKind(t *testing.T) {
	{
		// String
		{
			assert.Equal(t, "VARCHAR(MAX)", RedshiftDialect{}.DataTypeForKind(typing.String, true))
		}
		{
			assert.Equal(t, "VARCHAR(12345)", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}, false))
		}
	}
	{
		// Integers
		{
			// Small int
			assert.Equal(t, "INT2", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, false))
		}
		{
			// Integer
			assert.Equal(t, "INT4", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, false))
		}
		{
			// Big integer
			assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, false))
		}
		{
			// Not specified
			{
				// Literal
				assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.NotSpecifiedKind)}, false))
			}
			{
				assert.Equal(t, "INT8", RedshiftDialect{}.DataTypeForKind(typing.Integer, false))
			}
		}
	}
}

func TestRedshiftDialect_KindForDataType(t *testing.T) {
	dialect := RedshiftDialect{}
	{
		// Integers
		{
			// Small integer
			kd, err := dialect.KindForDataType("smallint", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, kd)
		}
		{
			{
				// Regular integers (upper)
				kd, err := dialect.KindForDataType("INTEGER", "")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
			{
				// Regular integers (lower)
				kd, err := dialect.KindForDataType("integer", "")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
		}
		{
			// Big integer
			kd, err := dialect.KindForDataType("bigint", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, kd)
		}
	}
	{
		// Double
		{
			kd, err := dialect.KindForDataType("double precision", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
		{
			kd, err := dialect.KindForDataType("DOUBLE precision", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
	}
	{
		// Numeric
		{
			kd, err := dialect.KindForDataType("numeric(5,2)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			kd, err := dialect.KindForDataType("numeric(5,5)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Boolean
		kd, err := dialect.KindForDataType("boolean", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// String with precision
		kd, err := dialect.KindForDataType("character varying", "65535")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(65535))}, kd)
	}
	{
		// Times
		{
			kd, err := dialect.KindForDataType("timestamp with time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimestampTzKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("timestamp without time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimestampTzKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("time without time zone", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.TimeKindType, kd.ExtendedTimeDetails.Type)
		}
		{
			kd, err := dialect.KindForDataType("date", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.ETime.Kind, kd.Kind)
			assert.Equal(t, ext.DateKindType, kd.ExtendedTimeDetails.Type)
		}
	}
}
