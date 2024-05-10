package sql

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestMSSQLDialect_QuoteIdentifier(t *testing.T) {
	dialect := MSSQLDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}

func TestMSSQLDialect_DataTypeForKind(t *testing.T) {
	tcs := []struct {
		kd typing.KindDetails
		// MSSQL is sensitive based on primary key
		expected     string
		expectedIsPk string
	}{
		{
			kd:           typing.String,
			expected:     "VARCHAR(MAX)",
			expectedIsPk: "VARCHAR(900)",
		},
		{
			kd: typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: ptr.ToInt(12345),
			},
			expected:     "VARCHAR(12345)",
			expectedIsPk: "VARCHAR(900)",
		},
	}

	for idx, tc := range tcs {
		assert.Equal(t, tc.expected, MSSQLDialect{}.DataTypeForKind(tc.kd, false), idx)
		assert.Equal(t, tc.expectedIsPk, MSSQLDialect{}.DataTypeForKind(tc.kd, true), idx)
	}
}

func TestMSSQLDialect_KindForDataType(t *testing.T) {
	dialect := MSSQLDialect{}

	colToExpectedKind := map[string]typing.KindDetails{
		"char":      typing.String,
		"varchar":   typing.String,
		"nchar":     typing.String,
		"nvarchar":  typing.String,
		"ntext":     typing.String,
		"text":      typing.String,
		"smallint":  typing.Integer,
		"tinyint":   typing.Integer,
		"int":       typing.Integer,
		"float":     typing.Float,
		"real":      typing.Float,
		"bit":       typing.Boolean,
		"date":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		"time":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		"datetime":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"datetime2": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	}

	for col, expectedKind := range colToExpectedKind {
		kd, err := dialect.KindForDataType(col, "")
		assert.NoError(t, err)
		assert.Equal(t, expectedKind.Kind, kd.Kind, col)
	}

	{
		_, err := dialect.KindForDataType("numeric(5", "")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
	{
		kd, err := dialect.KindForDataType("numeric(5, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("char", "5")
		assert.NoError(t, err)
		assert.Equal(t, typing.String.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.OptionalStringPrecision)
	}
}

func TestMSSQLDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedResult bool
	}{
		{
			name:           "MSSQL, table already exist error",
			err:            fmt.Errorf(`There is already an object named 'customers' in the database.`),
			expectedResult: true,
		},
		{
			name:           "MSSQL, column already exists error",
			err:            fmt.Errorf("Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once."),
			expectedResult: true,
		},
		{
			name: "MSSQL, random error",
			err:  fmt.Errorf("hello there qux"),
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expectedResult, MSSQLDialect{}.IsColumnAlreadyExistsErr(tc.err), tc.name)
	}
}


func TestMSSQLDialect_BuildCreateTableQuery(t *testing.T) {
	// Temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery("{TABLE}", true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery("{TABLE}", false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestMSSQLDialect_BuildAlterColumnQuery(t *testing.T) {
	assert.Equal(t,
		"ALTER TABLE {TABLE} drop {SQL_PART}",
		MSSQLDialect{}.BuildAlterColumnQuery("{TABLE}", constants.Delete, "{SQL_PART}"),
	)
}
