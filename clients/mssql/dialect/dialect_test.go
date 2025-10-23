package dialect

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestMSSQLDialect_QuoteIdentifier(t *testing.T) {
	dialect := MSSQLDialect{}

	expectedValueMap := map[string]string{
		"foo":       `[foo]`,
		"FOO":       `[FOO]`,
		`FOO"; BAD`: `[FOO"; BAD]`,
		`[ESCAPED]`: `[ESCAPED]`,
	}

	for key, expectedValue := range expectedValueMap {
		assert.Equal(t, expectedValue, dialect.QuoteIdentifier(key), key)
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

func TestMSSQLDialect_BuildDropTableQuery(t *testing.T) {
	assert.Equal(t,
		`DROP TABLE IF EXISTS [schema1].[table1]`,
		MSSQLDialect{}.BuildDropTableQuery(NewTableIdentifier("schema1", "table1")),
	)
}

func TestMSSQLDialect_BuildTruncateTableQuery(t *testing.T) {
	assert.Equal(t,
		`TRUNCATE TABLE [schema1].[table1]`,
		MSSQLDialect{}.BuildTruncateTableQuery(NewTableIdentifier("schema1", "table1")),
	)
}

func TestMSSQLDialect_BuildAddColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} ADD {SQL_PART}",
		MSSQLDialect{}.BuildAddColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestMSSQLDialect_BuildDropColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} DROP {SQL_PART}",
		MSSQLDialect{}.BuildDropColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestMSSQLDialect_BuildIsNotToastValueExpression(t *testing.T) {
	assert.Equal(t,
		`COALESCE(tbl.[bar], '') NOT LIKE '%__debezium_unavailable_value%'`,
		MSSQLDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
	)
	assert.Equal(t,
		`COALESCE(tbl.[foo], '') NOT LIKE '%__debezium_unavailable_value%'`,
		MSSQLDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
	)
}

func TestMSSQLDialect_BuildMergeQueries(t *testing.T) {
	_cols := []columns.Column{
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
USING database.schema.table AS stg ON tgt.[id] = stg.[id]
WHEN MATCHED AND stg.[__artie_delete] = 1 THEN DELETE
WHEN MATCHED AND COALESCE(stg.[__artie_delete], 0) = 0 THEN UPDATE SET [id]=stg.[id],[bar]=stg.[bar],[updated_at]=stg.[updated_at],[start]=stg.[start]
WHEN NOT MATCHED AND COALESCE(stg.[__artie_delete], 1) = 0 THEN INSERT ([id],[bar],[updated_at],[start]) VALUES (stg.[id],stg.[bar],stg.[updated_at],stg.[start]);`, queries[0])
	}
	{
		// Soft delete:
		queries, err := MSSQLDialect{}.BuildMergeQueries(
			fakeID,
			"{SUB_QUERY}",
			[]columns.Column{_cols[0]},
			[]string{},
			_cols,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Len(t, queries, 3)
		assert.Equal(t, `
INSERT INTO database.schema.table ([id],[bar],[updated_at],[start],[__artie_delete])
SELECT stg.[id],stg.[bar],stg.[updated_at],stg.[start],stg.[__artie_delete] FROM {SUB_QUERY} AS stg
LEFT JOIN database.schema.table AS tgt ON tgt.[id] = stg.[id]
WHERE tgt.[id] IS NULL;`, queries[0])
		assert.Equal(t, `
UPDATE tgt SET [id]=stg.[id],[bar]=stg.[bar],[updated_at]=stg.[updated_at],[start]=stg.[start],[__artie_delete]=stg.[__artie_delete]
FROM {SUB_QUERY} AS stg LEFT JOIN database.schema.table AS tgt ON tgt.[id] = stg.[id]
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 0;`, queries[1])
		assert.Equal(t, `
UPDATE tgt SET [__artie_delete]=stg.[__artie_delete]
FROM {SUB_QUERY} AS stg LEFT JOIN database.schema.table AS tgt ON tgt.[id] = stg.[id]
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 1;`, queries[2])
	}
}
