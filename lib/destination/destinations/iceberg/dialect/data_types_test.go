package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestIcebergDialect_DataTypeForKind(t *testing.T) {
	_dialect := IcebergDialect{}

	// Boolean
	assert.Equal(t, "BOOLEAN", _dialect.DataTypeForKind(typing.Boolean, false, config.SharedDestinationColumnSettings{}))

	{
		// String and related data types
		for _, kd := range []typing.KindDetails{typing.String, typing.Time, typing.Array, typing.Struct} {
			assert.Equal(t, "STRING", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
	}
	{
		// Float
		assert.Equal(t, "DOUBLE", _dialect.DataTypeForKind(typing.Float, false, config.SharedDestinationColumnSettings{}))

		// EDecimal
		{
			// DECIMAL(5, 2)
			kd := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2))
			assert.Equal(t, "DECIMAL(5, 2)", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// DECIMAL(39, 2) - Exceeds the max precision, so it will become a string.
			kd := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(39, 2))
			assert.Equal(t, "STRING", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}

		// Integers
		{
			// Not specified
			assert.Equal(t, "LONG", _dialect.DataTypeForKind(typing.Integer, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// SmallIntegerKind
			kd := typing.Integer
			kd.OptionalIntegerKind = typing.ToPtr(typing.SmallIntegerKind)
			assert.Equal(t, "INTEGER", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// IntegerKind
			kd := typing.Integer
			kd.OptionalIntegerKind = typing.ToPtr(typing.IntegerKind)
			assert.Equal(t, "INTEGER", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
		{
			// BigIntegerKind
			kd := typing.Integer
			kd.OptionalIntegerKind = typing.ToPtr(typing.BigIntegerKind)
			assert.Equal(t, "LONG", _dialect.DataTypeForKind(kd, false, config.SharedDestinationColumnSettings{}))
		}
	}

	// Date
	assert.Equal(t, "DATE", _dialect.DataTypeForKind(typing.Date, false, config.SharedDestinationColumnSettings{}))

	// TimestampNTZ
	assert.Equal(t, "TIMESTAMP_NTZ", _dialect.DataTypeForKind(typing.TimestampNTZ, false, config.SharedDestinationColumnSettings{}))

	// TimestampTZ
	assert.Equal(t, "TIMESTAMP", _dialect.DataTypeForKind(typing.TimestampTZ, false, config.SharedDestinationColumnSettings{}))
}

func TestIcebergDialect_KindForDataType(t *testing.T) {
	_dialect := IcebergDialect{}
	{
		// Boolean
		kd, err := _dialect.KindForDataType("BOOLEAN", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Decimal (10, 2)
		kd, err := _dialect.KindForDataType("DECIMAL(10, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, decimal.NewDetails(10, 2), *kd.ExtendedDecimalDetails)
	}
	{
		// INTEGER
		kd, err := _dialect.KindForDataType("INTEGER", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Integer.Kind, kd.Kind)
		assert.Equal(t, typing.ToPtr(typing.IntegerKind), kd.OptionalIntegerKind)
	}
	{
		// LONG
		kd, err := _dialect.KindForDataType("LONG", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Integer.Kind, kd.Kind)
		assert.Equal(t, typing.ToPtr(typing.BigIntegerKind), kd.OptionalIntegerKind)
	}
	{
		// Float and Double
		for _, kind := range []string{"FLOAT", "DOUBLE"} {
			kd, err := _dialect.KindForDataType(kind, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Float, kd)
		}
	}
	{
		// Date
		kd, err := _dialect.KindForDataType("DATE", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Date, kd)
	}
	{
		// TimestampTZ
		kd, err := _dialect.KindForDataType("TIMESTAMP", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kd)
	}
	{
		// TimestampNTZ
		kd, err := _dialect.KindForDataType("TIMESTAMP_NTZ", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kd)
	}
	{
		// String and other data types that map to a string.
		for _, kind := range []string{"STRING", "BINARY", "UUID", "FIXED"} {
			kd, err := _dialect.KindForDataType(kind, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
	}
}
