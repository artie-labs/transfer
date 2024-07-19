package dialect

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
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
				OptionalStringPrecision: ptr.ToInt32(12345),
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
		assert.Equal(t, int32(5), kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("char", "5")
		assert.NoError(t, err)
		assert.Equal(t, typing.String.Kind, kd.Kind)
		assert.Equal(t, int32(5), *kd.OptionalStringPrecision)
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
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestMSSQLDialect_BuildAlterColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} drop {SQL_PART}",
		MSSQLDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Delete, "{SQL_PART}"),
	)
}

func TestMSSQLDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		`COALESCE(tbl."bar", '') != '__debezium_unavailable_value'`,
		MSSQLDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		`COALESCE(tbl."foo", {}) != {'key': '__debezium_unavailable_value'}`,
		MSSQLDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
	)
}

func TestMSSQLDialect_BuildMergeQueries(t *testing.T) {
	var _cols = []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("bar", typing.String),
		columns.NewColumn("updated_at", typing.String),
		columns.NewColumn("start", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	fqTable := "database.schema.table"
	fakeID := &mocks.FakeTableIdentifier{}
	fakeID.FullyQualifiedNameReturns(fqTable)

	{
		queries, err := MSSQLDialect{}.BuildMergeQueries(
			fakeID,
			fqTable,
			"",
			[]columns.Column{_cols[0]},
			[]string{},
			_cols,
			false,
			false,
		)
		assert.NoError(t, err)
		assert.Len(t, queries, 1)
		assert.Equal(t, `
MERGE INTO database.schema.table tgt
USING database.schema.table AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND stg."__artie_delete" = 1 THEN DELETE
WHEN MATCHED AND COALESCE(stg."__artie_delete", 0) = 0 THEN UPDATE SET "id"=stg."id","bar"=stg."bar","updated_at"=stg."updated_at","start"=stg."start"
WHEN NOT MATCHED AND COALESCE(stg."__artie_delete", 1) = 0 THEN INSERT ("id","bar","updated_at","start") VALUES (stg."id",stg."bar",stg."updated_at",stg."start");`, queries[0])
	}
	{
		// Idempotent key:
		queries, err := MSSQLDialect{}.BuildMergeQueries(
			fakeID,
			"{SUB_QUERY}",
			"idempotent_key",
			[]columns.Column{_cols[0]},
			[]string{},
			_cols,
			false,
			false,
		)
		assert.NoError(t, err)
		assert.Len(t, queries, 1)
		assert.Equal(t, `
MERGE INTO database.schema.table tgt
USING {SUB_QUERY} AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND stg."__artie_delete" = 1 THEN DELETE
WHEN MATCHED AND COALESCE(stg."__artie_delete", 0) = 0 AND stg.idempotent_key >= tgt.idempotent_key THEN UPDATE SET "id"=stg."id","bar"=stg."bar","updated_at"=stg."updated_at","start"=stg."start"
WHEN NOT MATCHED AND COALESCE(stg."__artie_delete", 1) = 0 THEN INSERT ("id","bar","updated_at","start") VALUES (stg."id",stg."bar",stg."updated_at",stg."start");`, queries[0])
	}
	{
		// Soft delete:
		queries, err := MSSQLDialect{}.BuildMergeQueries(
			fakeID,
			"{SUB_QUERY}",
			"",
			[]columns.Column{_cols[0]},
			[]string{},
			_cols,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Len(t, queries, 1)
		assert.Equal(t, `
MERGE INTO database.schema.table tgt
USING {SUB_QUERY} AS stg ON tgt."id" = stg."id"
WHEN MATCHED THEN UPDATE SET "id"=stg."id","bar"=stg."bar","updated_at"=stg."updated_at","start"=stg."start","__artie_delete"=stg."__artie_delete"
WHEN NOT MATCHED THEN INSERT ("id","bar","updated_at","start","__artie_delete") VALUES (stg."id",stg."bar",stg."updated_at",stg."start",stg."__artie_delete");`, queries[0])
	}
}
