package sql

import (
	"fmt"
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

func TestDialect_DataTypeForKind(t *testing.T) {
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

func TestDialect_IsColumnAlreadyExistErrs(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		dialect        Dialect
		expectedResult bool
	}{
		{
			name:           "Redshift actual error",
			dialect:        RedshiftDialect{},
			err:            fmt.Errorf(`ERROR: column "foo" of relation "statement" already exists [ErrorId: 1-64da9ea9]`),
			expectedResult: true,
		},
		{
			name:    "Redshift error, but irrelevant",
			dialect: RedshiftDialect{},
			err:     fmt.Errorf("foo"),
		},
		{
			name:           "MSSQL, table already exist error",
			dialect:        MSSQLDialect{},
			err:            fmt.Errorf(`There is already an object named 'customers' in the database.`),
			expectedResult: true,
		},
		{
			name:           "MSSQL, column already exists error",
			dialect:        MSSQLDialect{},
			err:            fmt.Errorf("Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once."),
			expectedResult: true,
		},
		{
			name:    "MSSQL, random error",
			err:     fmt.Errorf("hello there qux"),
			dialect: MSSQLDialect{},
		},
		{
			name:           "BigQuery, column already exists error",
			dialect:        BigQueryDialect{},
			err:            fmt.Errorf("Column already exists"),
			expectedResult: true,
		},
		{
			name:    "BigQuery, random error",
			dialect: BigQueryDialect{},
			err:     fmt.Errorf("hello there qux"),
		},
		{
			name:           "Snowflake, column already exists error",
			dialect:        SnowflakeDialect{},
			err:            fmt.Errorf("Column already exists"),
			expectedResult: true,
		},
		{
			name:    "Snowflake, random error",
			dialect: SnowflakeDialect{},
			err:     fmt.Errorf("hello there qux"),
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expectedResult, tc.dialect.IsColumnAlreadyExistsErr(tc.err), tc.name)
	}
}
