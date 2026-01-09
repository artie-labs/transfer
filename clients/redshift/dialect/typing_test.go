package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func buildInteger(optionalKind typing.OptionalIntegerKind) typing.KindDetails {
	kd := typing.Integer
	kd.OptionalIntegerKind = typing.ToPtr(optionalKind)
	return kd
}

func TestRedshiftDialect_DataTypeForKind(t *testing.T) {
	expectedKindDetailsToValueMap := map[typing.KindDetails]string{
		// String:
		typing.String: "VARCHAR(MAX)",
		{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}: "VARCHAR(12345)",
		// Integers:
		buildInteger(typing.SmallIntegerKind): "INT2",
		buildInteger(typing.IntegerKind):      "INT4",
		buildInteger(typing.BigIntegerKind):   "INT8",
		buildInteger(typing.NotSpecifiedKind): "INT8",
		typing.Integer:                        "INT8",
		// Timestamps:
		typing.TimestampTZ:  "TIMESTAMP WITH TIME ZONE",
		typing.TimestampNTZ: "TIMESTAMP WITHOUT TIME ZONE",
	}

	for kd, expected := range expectedKindDetailsToValueMap {
		actual, err := RedshiftDialect{}.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

func TestRedshiftDialect_KindForDataType(t *testing.T) {
	dialect := RedshiftDialect{}
	{
		// Invalid
		_, err := dialect.KindForDataType("invalid")
		assert.True(t, typing.IsUnsupportedDataTypeError(err))
	}
	{
		// Integers
		{
			// Small integer
			kd, err := dialect.KindForDataType("smallint")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.SmallIntegerKind)}, kd)
		}
		{
			{
				// Regular integers (upper)
				kd, err := dialect.KindForDataType("INTEGER")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
			{
				// Regular integers (lower)
				kd, err := dialect.KindForDataType("integer")
				assert.NoError(t, err)
				assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.IntegerKind)}, kd)
			}
		}
		{
			// Big integer
			kd, err := dialect.KindForDataType("bigint")
			assert.NoError(t, err)
			assert.Equal(t, typing.KindDetails{Kind: typing.Integer.Kind, OptionalIntegerKind: typing.ToPtr(typing.BigIntegerKind)}, kd)
		}
	}
	{
		// Double
		doubleTypeMap := map[string]typing.KindDetails{
			"double precision": typing.Float,
			"DOUBLE precision": typing.Float,
		}

		for rawType, expectedKind := range doubleTypeMap {
			kd, err := dialect.KindForDataType(rawType)
			assert.NoError(t, err)
			assert.Equal(t, expectedKind, kd)
		}
	}
	{
		// Numeric
		{
			kd, err := dialect.KindForDataType("numeric(5,2)")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			kd, err := dialect.KindForDataType("numeric(5,5)")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Boolean
		kd, err := dialect.KindForDataType("boolean")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// String with precision
		kd, err := dialect.KindForDataType("character varying(65535)")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(65535))}, kd)
	}
	{
		// Character
		kd, err := dialect.KindForDataType("character")
		assert.NoError(t, err)
		assert.Equal(t, typing.KindDetails{Kind: typing.String.Kind}, kd)
	}
	{
		// Times
		{
			// TimestampTZ
			kd, err := dialect.KindForDataType("timestamp with time zone")
			assert.NoError(t, err)
			assert.Equal(t, typing.TimestampTZ, kd)
		}
		{
			// TimestampNTZ
			kd, err := dialect.KindForDataType("timestamp without time zone")
			assert.NoError(t, err)
			assert.Equal(t, typing.TimestampNTZ, kd)
		}
		{
			// Time
			kd, err := dialect.KindForDataType("time without time zone")
			assert.NoError(t, err)
			assert.Equal(t, typing.TimeKindDetails, kd)
		}
		{
			// Date
			kd, err := dialect.KindForDataType("date")
			assert.NoError(t, err)
			assert.Equal(t, typing.Date, kd)
		}
	}
}
