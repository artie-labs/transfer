package typing

import (
	"strings"
	"testing"

	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeTypeToKindNumber(t *testing.T) {
	{
		expectedIntegers := []string{"number(38, 0)", "number(2, 0)", "number(3, 0)"}
		for _, expectedInteger := range expectedIntegers {
			kd := SnowflakeTypeToKind(expectedInteger)
			assert.Equal(t, Integer, kd, expectedInteger)
		}
	}
	{
		expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number(2, 30)"}
		for _, expectedFloat := range expectedFloats {
			kd := SnowflakeTypeToKind(expectedFloat)
			assert.Equal(t, EDecimal.Kind, kd.Kind, expectedFloat)
		}
	}
}

func TestSnowflakeTypeToKindFloats(t *testing.T) {
	{
		expectedFloats := []string{"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
			"DOUBLE PRECISION", "REAL"}
		for _, expectedFloat := range expectedFloats {
			kd := SnowflakeTypeToKind(strings.ToLower(expectedFloat))
			assert.Equal(t, Float, kd, expectedFloat)
		}
	}
	{
		// Invalid because precision nor scale is included.
		kd := SnowflakeTypeToKind("NUMERIC")
		assert.Equal(t, Invalid, kd)
	}
	{
		expectedNumerics := []string{"NUMERIC(38, 2)", "NUMBER(38, 2)", "DECIMAL"}
		for _, expectedNumeric := range expectedNumerics {
			kd := SnowflakeTypeToKind(strings.ToLower(expectedNumeric))
			assert.Equal(t, EDecimal.Kind, kd.Kind, expectedNumeric)
		}
	}
}

func TestSnowflakeTypeToKindInteger(t *testing.T) {
	expectedIntegers := []string{"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}
	for _, expectedInteger := range expectedIntegers {
		kd := SnowflakeTypeToKind(strings.ToLower(expectedInteger))
		assert.Equal(t, Integer, kd, expectedInteger)
	}
}

func TestSnowflakeTypeToKindOther(t *testing.T) {
	expectedStrings := []string{"VARCHAR (255)", "CHARACTER", "CHAR", "STRING", "TEXT"}
	for _, expectedString := range expectedStrings {
		kd := SnowflakeTypeToKind(strings.ToLower(expectedString))
		assert.Equal(t, String, kd, expectedString)
	}
}

func TestSnowflakeTypeToKindDateTime(t *testing.T) {
	expectedDateTimes := []string{"DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ(9)", "TIMESTAMP_TZ"}
	for _, expectedDateTime := range expectedDateTimes {
		kd := SnowflakeTypeToKind(expectedDateTime)
		assert.Equal(t, ext.DateTime.Type, kd.ExtendedTimeDetails.Type, expectedDateTime)
	}
}

func TestSnowflakeTypeToKindComplex(t *testing.T) {
	{
		expectedStructs := []string{"variant", "VaRIANT", "OBJECT"}
		for _, expectedStruct := range expectedStructs {
			kd := SnowflakeTypeToKind(expectedStruct)
			assert.Equal(t, Struct, kd, expectedStruct)
		}
	}
	{
		kd := SnowflakeTypeToKind("boolean")
		assert.Equal(t, Boolean, kd)
	}
	{
		kd := SnowflakeTypeToKind("ARRAY")
		assert.Equal(t, Array, kd)
	}
}

func TestSnowflakeTypeNoDataLoss(t *testing.T) {
	kindDetails := []KindDetails{
		NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.TimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.DateKindType),
		String,
		Boolean,
		Struct,
	}

	for _, kindDetail := range kindDetails {
		kd := SnowflakeTypeToKind(KindToSnowflake(kindDetail))
		assert.Equal(t, kindDetail, kd)
	}
}
