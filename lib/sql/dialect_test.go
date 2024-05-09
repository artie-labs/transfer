package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestMSSQLDialect_QuoteIdentifier(t *testing.T) {
	dialect := MSSQLDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}

func TestBigQueryDialect_QuoteIdentifier(t *testing.T) {
	dialect := BigQueryDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
}

func TestRedshiftDialect_QuoteIdentifier(t *testing.T) {
	dialect := RedshiftDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("FOO"))
}

func TestSnowflakeDialect_QuoteIdentifier(t *testing.T) {
	dialect := SnowflakeDialect{}
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}

func Test_DataTypeForKind(t *testing.T) {
	type _tc struct {
		kd                    typing.KindDetails
		expectedSnowflakeType string
		expectedBigQueryType  string
		expectedRedshiftType  string

		// MSSQL is sensitive based on primary key
		expectedMSSQLType   string
		expectedMSSQLTypePk string
	}

	tcs := []_tc{
		{
			kd:                    typing.String,
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(MAX)",
			expectedMSSQLType:     "VARCHAR(MAX)",
			expectedMSSQLTypePk:   "VARCHAR(900)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt(12345),
			},
			expectedSnowflakeType: "string",
			expectedBigQueryType:  "string",
			expectedRedshiftType:  "VARCHAR(12345)",
			expectedMSSQLType:     "VARCHAR(12345)",
			expectedMSSQLTypePk:   "VARCHAR(900)",
		},
	}

	for idx, tc := range tcs {
		for _, isPk := range []bool{true, false} {
			assert.Equal(t, tc.expectedSnowflakeType, SnowflakeDialect{}.DataTypeForKind(tc.kd, isPk), idx)
			assert.Equal(t, tc.expectedBigQueryType, BigQueryDialect{}.DataTypeForKind(tc.kd, isPk), idx)
			assert.Equal(t, tc.expectedRedshiftType, RedshiftDialect{}.DataTypeForKind(tc.kd, isPk), idx)
		}

		assert.Equal(t, tc.expectedMSSQLType, MSSQLDialect{}.DataTypeForKind(tc.kd, false), idx)
		assert.Equal(t, tc.expectedMSSQLTypePk, MSSQLDialect{}.DataTypeForKind(tc.kd, true), idx)
	}
}
