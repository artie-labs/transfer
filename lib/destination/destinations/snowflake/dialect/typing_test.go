package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestSnowflakeDialect_DataTypeForKind(t *testing.T) {
	{
		// String
		{
			assert.Equal(t, "string", SnowflakeDialect{}.DataTypeForKind(typing.String, false, config.SharedDestinationColumnSettings{}))
		}
		{
			assert.Equal(t, "string", SnowflakeDialect{}.DataTypeForKind(typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(12345))}, false, config.SharedDestinationColumnSettings{}))
		}
	}
}

func TestSnowflakeDialect_KindForDataType_Number(t *testing.T) {
	{
		// Integers
		{
			// number(38, 0)
			kd, err := SnowflakeDialect{}.KindForDataType("number(38, 0)", "")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(38, 0)", kd.ExtendedDecimalDetails.SnowflakeKind())
		}
		{
			// number(2, 0)
			kd, err := SnowflakeDialect{}.KindForDataType("number(2, 0)", "")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(2, 0)", kd.ExtendedDecimalDetails.SnowflakeKind())
		}
		{
			// number(3, 0)
			kd, err := SnowflakeDialect{}.KindForDataType("number(3, 0)", "")
			assert.NoError(t, err)

			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(3), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(0), kd.ExtendedDecimalDetails.Scale())
			assert.Equal(t, "NUMERIC(3, 0)", kd.ExtendedDecimalDetails.SnowflakeKind())
		}
	}
	{
		expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number(2, 30)"}
		for _, expectedFloat := range expectedFloats {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind, expectedFloat)
		}
	}
}

func TestSnowflakeDialect_KindForDataType(t *testing.T) {
	{
		// Invalid
		{
			kd, err := SnowflakeDialect{}.KindForDataType("", "")
			assert.ErrorContains(t, err, `unsupported data type: ""`)
			assert.Equal(t, typing.Invalid, kd)
		}
		{
			kd, err := SnowflakeDialect{}.KindForDataType("abc123", "")
			assert.ErrorContains(t, err, `unsupported data type: "abc123"`)
			assert.Equal(t, typing.Invalid, kd)
		}
	}
	{
		// Booleans
		kd, err := SnowflakeDialect{}.KindForDataType("boolean", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		// Floats
		{
			expectedFloats := []string{"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"}
			for _, expectedFloat := range expectedFloats {
				kd, err := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
				assert.NoError(t, err)
				assert.Equal(t, typing.Float, kd, expectedFloat)
			}
		}
		{
			// Invalid because precision nor scale is included.
			kd, err := SnowflakeDialect{}.KindForDataType("NUMERIC", "")
			assert.ErrorContains(t, err, "invalid number of parts: 0")
			assert.Equal(t, typing.Invalid, kd)
		}
		{
			kd, err := SnowflakeDialect{}.KindForDataType("NUMERIC(38, 2)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			kd, err := SnowflakeDialect{}.KindForDataType("NUMBER(38, 2)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
			assert.Equal(t, int32(38), kd.ExtendedDecimalDetails.Precision())
			assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
		}
		{
			kd, err := SnowflakeDialect{}.KindForDataType("DECIMAL", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		}
	}
	{
		// Integers
		expectedIntegers := []string{"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}
		for _, expectedInteger := range expectedIntegers {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedInteger, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Integer, kd, expectedInteger)
		}
	}
	{
		// String
		expectedStrings := []string{"CHARACTER", "CHAR", "STRING", "TEXT"}
		for _, expectedString := range expectedStrings {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedString, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.String, kd, expectedString)
		}

		{
			kd, err := SnowflakeDialect{}.KindForDataType("VARCHAR (255)", "")
			assert.NoError(t, err)
			assert.Equal(t, typing.String.Kind, kd.Kind)
			assert.Equal(t, int32(255), *kd.OptionalStringPrecision)
		}
	}
	{
		// Structs
		expectedStructs := []string{"variant", "VaRIANT", "OBJECT"}
		for _, expectedStruct := range expectedStructs {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedStruct, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.Struct, kd, expectedStruct)
		}
	}
	{
		// Arrays
		kd, err := SnowflakeDialect{}.KindForDataType("ARRAY", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.Array, kd)
	}
}

func TestSnowflakeDialect_KindForDataType_DateTime(t *testing.T) {
	{
		// Timestamp with time zone
		expectedDateTimes := []string{"TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
		for _, expectedDateTime := range expectedDateTimes {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedDateTime, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.TimestampTZ, kd, expectedDateTime)
		}
	}
	{
		// Timestamp without time zone
		expectedDateTimes := []string{"TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ(9)"}
		for _, expectedDateTime := range expectedDateTimes {
			kd, err := SnowflakeDialect{}.KindForDataType(expectedDateTime, "")
			assert.NoError(t, err)
			assert.Equal(t, typing.TimestampNTZ, kd, expectedDateTime)
		}
	}
}

func TestSnowflakeDialect_KindForDataType_NoDataLoss(t *testing.T) {
	kindDetails := []typing.KindDetails{
		typing.TimestampTZ,
		typing.Time,
		typing.Date,
		typing.String,
		typing.Boolean,
		typing.Struct,
	}

	for _, kindDetail := range kindDetails {
		kd, err := SnowflakeDialect{}.KindForDataType(SnowflakeDialect{}.DataTypeForKind(kindDetail, false, config.SharedDestinationColumnSettings{}), "")
		assert.NoError(t, err)
		assert.Equal(t, kindDetail, kd)
	}
}
