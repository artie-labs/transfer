package typing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSnowflakeTypeToKindNumber(t *testing.T) {
	expectedIntegers := []string{"number(38, 0)", "number(2, 0)", "number (3, 0)"}
	for _, expectedInteger := range expectedIntegers {
		assert.Equal(t, SnowflakeTypeToKind(expectedInteger), Integer, expectedInteger)
	}

	expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number (2, 30)"}
	for _, expectedFloat := range expectedFloats {
		assert.Equal(t, SnowflakeTypeToKind(expectedFloat), Float, expectedFloat)
	}
}

func TestSnowflakeTypeToKindFloats(t *testing.T) {
	expectedFloats := []string{"NUMBER(38, 2)", "DECIMAL", "NUMERIC", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
		"DOUBLE PRECISION", "REAL"}
	for _, expectedFloat := range expectedFloats {
		assert.Equal(t, SnowflakeTypeToKind(expectedFloat), Float, expectedFloat)
	}
}

func TestSnowflakeTypeToKindInteger(t *testing.T) {
	expectedIntegers := []string{"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"}
	for _, expectedInteger := range expectedIntegers {
		assert.Equal(t, SnowflakeTypeToKind(expectedInteger), Integer, expectedInteger)
	}
}

func TestSnowflakeTypeToKindOther(t *testing.T) {
	expectedStrings := []string{"VARCHAR (255)", "CHARACTER", "CHAR", "STRING", "TEXT"}
	for _, expectedString := range expectedStrings {
		assert.Equal(t, SnowflakeTypeToKind(expectedString), String, expectedString)
	}
}

func TestSnowflakeTypeToKindDateTime(t *testing.T) {
	expectedDateTimes := []string{"DATETIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ(9)", "TIMESTAMP_TZ"}
	for _, expectedDateTime := range expectedDateTimes {
		assert.Equal(t, SnowflakeTypeToKind(expectedDateTime).ExtendedTimeDetails.Type, DateTime.Type, expectedDateTime)
	}
}

func TestSnowflakeTypeToKindComplex(t *testing.T) {
	expectedStructs := []string{"variant", "VaRIANT", "OBJECT"}
	for _, expectedStruct := range expectedStructs {
		assert.Equal(t, SnowflakeTypeToKind(expectedStruct), Struct, expectedStruct)
	}

	assert.Equal(t, SnowflakeTypeToKind("boolean"), Boolean)
	assert.Equal(t, SnowflakeTypeToKind("ARRAY"), Array)
}

func TestSnowflakeTypeToKindErrors(t *testing.T) {
	assert.Equal(t, SnowflakeTypeToKind(""), Invalid)
	assert.Equal(t, SnowflakeTypeToKind("abc123"), Invalid)
}
