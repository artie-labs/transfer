package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestBigQueryDialect_DataTypeForKind(t *testing.T) {
	{
		// String
		actual, err := BigQueryDialect{}.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "string", actual)

		actual, err = BigQueryDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}, true, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "string", actual)
	}
	{
		// NUMERIC
		// Precision and scale are not specified
		actual, err := BigQueryDialect{}.DataTypeForKind(typing.EDecimal, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)
		assert.Equal(t, "numeric", actual)
	}
}

func TestBigQueryDialect_KindForDataType_NoDataLoss(t *testing.T) {
	kindDetails := []typing.KindDetails{
		typing.TimestampTZ,
		typing.TimeKindDetails,
		typing.Date,
		typing.String,
		typing.Boolean,
		typing.Struct,
	}

	for _, kindDetail := range kindDetails {
		actual, err := BigQueryDialect{}.DataTypeForKind(kindDetail, false, config.SharedDestinationColumnSettings{})
		assert.NoError(t, err)

		kd, err := BigQueryDialect{}.KindForDataType(actual)
		assert.NoError(t, err)
		assert.Equal(t, kindDetail, kd)
	}
}

func TestBigQueryDialect_KindForDataType(t *testing.T) {
	dialect := BigQueryDialect{}
	{
		// Booleans
		for _, boolKind := range []string{"bool", "boolean"} {
			kd, err := dialect.KindForDataType(boolKind)
			assert.NoError(t, err)
			assert.Equal(t, typing.Boolean, kd)
		}
	}
	{
		// Strings
		for _, stringKind := range []string{"string", "varchar", "string (10)", "STrinG"} {
			kd, err := dialect.KindForDataType(stringKind)
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd)
		}
	}
	{
		// Numeric
		{
			// Invalid
			{
				kd, err := dialect.KindForDataType("numeric(1,2,3)")
				assert.ErrorContains(t, err, "invalid number of parts: 3")
				assert.Equal(t, typing.Invalid, kd)
			}
			{
				_, err := dialect.KindForDataType("numeric(5")
				assert.ErrorContains(t, err, "missing closing parenthesis")
			}
		}
		{
			// NUMERIC (no precision or scale)
			kd, err := dialect.KindForDataType("numeric")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(9), kd.ExtendedDecimalDetails.Scale())
		}
		{
			// Numeric(5)
			kd, err := dialect.KindForDataType("NUMERIC(5)")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(5, 0)", kd.ExtendedDecimalDetails.BigQueryKind(false))

		}
		{
			// Numeric(5, 0)
			kd, err := dialect.KindForDataType("NUMERIC(5, 0)")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(5, 0)", kd.ExtendedDecimalDetails.BigQueryKind(false))
		}
		{
			// Numeric(5, 2)
			kd, err := dialect.KindForDataType("numeric(5, 2)")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			// BigNumeric(5, 2)
			kd, err := dialect.KindForDataType("bignumeric(5, 2)")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			// BIGNUMERIC (no precision or scale)
			kd, err := dialect.KindForDataType("bignumeric")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(76), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Scale())
		}
	}
	{
		// Integers
		for _, intKind := range []string{"int", "integer", "inT64"} {
			kd, err := dialect.KindForDataType(intKind)
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd, intKind)
		}
	}
	{
		// Arrays
		for _, arrayKind := range []string{"array<integer>", "array<string>"} {
			kd, err := dialect.KindForDataType(arrayKind)
			assert.NoError(t, err)
			assert.Equal(t, typing.Array, kd, arrayKind)
		}
	}
	{
		// Structs
		for _, structKind := range []string{"struct<foo STRING>", "record", "json"} {
			kd, err := dialect.KindForDataType(structKind)
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd, structKind)
		}
	}
	{
		// Date
		kd, err := dialect.KindForDataType("date")
		assert.NoError(t, err)
		assert.Equal(t, typing.Date, kd)
	}
	{
		// Time
		kd, err := dialect.KindForDataType("time")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimeKindDetails, kd)
	}
	{
		// Timestamp (Timestamp TZ)
		kd, err := dialect.KindForDataType("timestamp")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampTZ, kd)
	}
	{
		// Datetime (Timestamp NTZ)
		kd, err := dialect.KindForDataType("datetime")
		assert.NoError(t, err)
		assert.Equal(t, typing.TimestampNTZ, kd)
	}
	{
		// Invalid types
		{
			kd, err := dialect.KindForDataType("")
			assert.ErrorContains(t, err, `unsupported data type: ""`)
			assert.Equal(t, typing.Invalid, kd)
		}
		{
			kd, err := dialect.KindForDataType("foo")
			assert.ErrorContains(t, err, `unsupported data type: "foo"`)
			assert.Equal(t, typing.Invalid, kd)
		}
	}
}
