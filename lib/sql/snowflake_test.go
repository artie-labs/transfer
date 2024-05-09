package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeDialect_KindForDataType_Number(t *testing.T) {
	{
		expectedIntegers := []string{"number(38, 0)", "number(2, 0)", "number(3, 0)"}
		for _, expectedInteger := range expectedIntegers {
			kd := SnowflakeDialect{}.KindForDataType(expectedInteger, "")
			assert.Equal(t, typing.Integer, kd, expectedInteger)
		}
	}
	{
		expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number(2, 30)"}
		for _, expectedFloat := range expectedFloats {
			kd := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind, expectedFloat)
		}
	}
}

func TestSnowflakeDialect_KindForDataType_Floats(t *testing.T) {
	{
		expectedFloats := []string{"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
			"DOUBLE PRECISION", "REAL"}
		for _, expectedFloat := range expectedFloats {
			kd := SnowflakeDialect{}.KindForDataType(expectedFloat, "")
			assert.Equal(t, typing.Float, kd, expectedFloat)
		}
	}
	{
		// Invalid because precision nor scale is included.
		kd := SnowflakeDialect{}.KindForDataType("NUMERIC", "")
		assert.Equal(t, typing.Invalid, kd)
	}
	{
		expectedNumerics := []string{"NUMERIC(38, 2)", "NUMBER(38, 2)", "DECIMAL"}
		for _, expectedNumeric := range expectedNumerics {
			kd := SnowflakeDialect{}.KindForDataType(expectedNumeric, "")
			assert.Equal(t, typing.EDecimal.Kind, kd.Kind, expectedNumeric)
		}
	}
}

func TestSnowflakeDialect_KindForDataType_Integer(t *testing.T) {
	expectedIntegers := []string{"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}
	for _, expectedInteger := range expectedIntegers {
		kd := SnowflakeDialect{}.KindForDataType(expectedInteger, "")
		assert.Equal(t, typing.Integer, kd, expectedInteger)
	}
}

func TestSnowflakeDialect_KindForDataType_Other(t *testing.T) {
	expectedStrings := []string{"VARCHAR (255)", "CHARACTER", "CHAR", "STRING", "TEXT"}
	for _, expectedString := range expectedStrings {
		kd := SnowflakeDialect{}.KindForDataType(expectedString, "")
		assert.Equal(t, typing.String, kd, expectedString)
	}
}

func TestSnowflakeDialect_KindForDataType_DateTime(t *testing.T) {
	expectedDateTimes := []string{"DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ(9)", "TIMESTAMP_TZ"}
	for _, expectedDateTime := range expectedDateTimes {
		kd := SnowflakeDialect{}.KindForDataType(expectedDateTime, "")
		assert.Equal(t, ext.DateTime.Type, kd.ExtendedTimeDetails.Type, expectedDateTime)
	}
}

func TestSnowflakeDialect_KindForDataType_Complex(t *testing.T) {
	{
		expectedStructs := []string{"variant", "VaRIANT", "OBJECT"}
		for _, expectedStruct := range expectedStructs {
			kd := SnowflakeDialect{}.KindForDataType(expectedStruct, "")
			assert.Equal(t, typing.Struct, kd, expectedStruct)
		}
	}
	{
		kd := SnowflakeDialect{}.KindForDataType("boolean", "")
		assert.Equal(t, typing.Boolean, kd)
	}
	{
		kd := SnowflakeDialect{}.KindForDataType("ARRAY", "")
		assert.Equal(t, typing.Array, kd)
	}
}

func TestSnowflakeDialect_KindForDataType_Errors(t *testing.T) {
	assert.Equal(t, typing.Invalid, SnowflakeDialect{}.KindForDataType("", ""))
	assert.Equal(t, typing.Invalid, SnowflakeDialect{}.KindForDataType("abc123", ""))
}

func TestSnowflakeTypeNoDataLoss(t *testing.T) {
	kindDetails := []typing.KindDetails{
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		typing.String,
		typing.Boolean,
		typing.Struct,
	}

	for _, kindDetail := range kindDetails {
		kd := SnowflakeDialect{}.KindForDataType(SnowflakeDialect{}.DataTypeForKind(kindDetail, false), "")
		assert.Equal(t, kindDetail, kd)
	}
}
