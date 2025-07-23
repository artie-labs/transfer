package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func toInteger(optionalKind typing.OptionalIntegerKind) typing.KindDetails {
	kd := typing.Integer
	kd.OptionalIntegerKind = typing.ToPtr(optionalKind)
	return kd
}

func TestIcebergDialect_DataTypeForKind(t *testing.T) {
	_dialect := IcebergDialect{}
	kindDetailsToValueMap := map[typing.KindDetails]string{
		// Boolean:
		typing.Boolean: "BOOLEAN",
		// String and related data types:
		typing.String: "STRING",
		typing.Time:   "STRING",
		typing.Array:  "STRING",
		typing.Struct: "STRING",
		// Float:
		typing.Float: "DOUBLE",
		// Decimals:
		typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2)): "DECIMAL(5, 2)",
		// Exceeds the max precision, so it'll become a string.
		typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 2)): "STRING",
		// Integers:
		typing.Integer:                     "LONG",
		toInteger(typing.SmallIntegerKind): "INTEGER",
		toInteger(typing.IntegerKind):      "INTEGER",
		toInteger(typing.BigIntegerKind):   "LONG",
		// Date and timestamp data types:
		typing.Date:         "DATE",
		typing.TimestampNTZ: "TIMESTAMP_NTZ",
		typing.TimestampTZ:  "TIMESTAMP",
	}

	for kd, expected := range kindDetailsToValueMap {
		actual, err := _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
}

func TestIcebergDialect_KindForDataType(t *testing.T) {
	_dialect := IcebergDialect{}
	{
		// Boolean
		kd, err := _dialect.KindForDataType("BOOLEAN")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Decimal (10, 2)
		kd, err := _dialect.KindForDataType("DECIMAL(10, 2)")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, decimal.NewDetails(10, 2), *kd.ExtendedDecimalDetails)
	}
	{
		// INTEGER
		kd, err := _dialect.KindForDataType("INTEGER")
		assert.NoError(t, err)
		assert.Equal(t, typing.Integer.Kind, kd.Kind)
		assert.Equal(t, typing.ToPtr(typing.IntegerKind), kd.OptionalIntegerKind)
	}
	{
		// LONG
		kd, err := _dialect.KindForDataType("LONG")
		assert.NoError(t, err)
		assert.Equal(t, typing.Integer.Kind, kd.Kind)
		assert.Equal(t, typing.ToPtr(typing.BigIntegerKind), kd.OptionalIntegerKind)
	}
	{
		// Float and Double
		for _, kind := range []string{"FLOAT", "DOUBLE"} {
			kd, err := _dialect.KindForDataType(kind)
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
	}
	{
		// Date
		kd, err := _dialect.KindForDataType("DATE")
		assert.NoError(t, err)
		assert.Equal(t, typing.Date, kd)
	}
	{
		// TimestampTZ
		kd, err := _dialect.KindForDataType("TIMESTAMP")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kd)
	}
	{
		// TimestampNTZ
		kd, err := _dialect.KindForDataType("TIMESTAMP_NTZ")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kd)
	}
	{
		// String and other data types that map to a string.
		for _, kind := range []string{"STRING", "BINARY", "UUID", "FIXED"} {
			kd, err := _dialect.KindForDataType(kind)
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
	}
}
