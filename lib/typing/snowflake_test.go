package typing

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeTypeToKindNumber(t *testing.T) {
	expectedIntegers := []string{"number(38, 0)", "number(2, 0)", "number(3, 0)"}
	for _, expectedInteger := range expectedIntegers {
		assert.Equal(t, SnowflakeTypeToKind(expectedInteger), Integer, expectedInteger)
	}

	expectedFloats := []string{"number(38, 1)", "number(2, 2)", "number(2, 30)"}
	for _, expectedFloat := range expectedFloats {
		assert.Equal(t, SnowflakeTypeToKind(expectedFloat).Kind, EDecimal.Kind, expectedFloat)
	}
}

func TestSnowflakeTypeToKindFloats(t *testing.T) {
	expectedFloats := []string{"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
		"DOUBLE PRECISION", "REAL"}
	for _, expectedFloat := range expectedFloats {
		assert.Equal(t, SnowflakeTypeToKind(expectedFloat), Float, expectedFloat)
	}

	// Invalid because precision nor scale is included.
	assert.Equal(t, SnowflakeTypeToKind("NUMERIC"), Invalid)

	expectedNumerics := []string{"NUMERIC(38, 2)", "NUMBER(38, 2)", "DECIMAL"}
	for _, expectedNumeric := range expectedNumerics {
		assert.Equal(t, SnowflakeTypeToKind(expectedNumeric).Kind, EDecimal.Kind, expectedNumeric)
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
		assert.Equal(t, SnowflakeTypeToKind(expectedDateTime).ExtendedTimeDetails.Type, ext.DateTime.Type, expectedDateTime)
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
		assert.Equal(t, kindDetail, SnowflakeTypeToKind(kindToSnowflake(kindDetail)))
	}
}
