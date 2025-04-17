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

func TestRedshiftDialect_QuoteIdentifier(t *testing.T) {
	dialect := RedshiftDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("FOO"))
	assert.Equal(t, `"foo; bad"`, dialect.QuoteIdentifier(`FOO"; BAD`))
}

func TestRedshiftDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	{
		// Irrelevant error
		assert.False(t, RedshiftDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("foo")))
	}
	{
		// Actual error
		assert.True(t, RedshiftDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf(`ERROR: column "foo" of relation "statement" already exists [ErrorId: 1-64da9ea9]`)))
	}
}

func TestRedshiftDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2});`,
		RedshiftDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2});`,
		RedshiftDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestRedshiftDialect_BuildDropTableQuery(t *testing.T) {
	assert.Equal(t,
		`DROP TABLE IF EXISTS schema1."table1"`,
		RedshiftDialect{}.BuildDropTableQuery(NewTableIdentifier("schema1", "table1")),
	)
}

func TestRedshiftDialect_BuildTruncateTableQuery(t *testing.T) {
	assert.Equal(t,
		`TRUNCATE TABLE schema1."table1"`,
		RedshiftDialect{}.BuildTruncateTableQuery(NewTableIdentifier("schema1", "table1")),
	)
}

func TestRedshiftDialect_BuildAddColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} ADD COLUMN {SQL_PART}",
		RedshiftDialect{}.BuildAddColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestRedshiftDialect_BuildDropColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} DROP COLUMN {SQL_PART}",
		RedshiftDialect{}.BuildDropColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestRedshiftDialect_BuildIsNotToastValueExpression(t *testing.T) {
	{
		// Typing is not specified
		assert.Equal(t,
			`COALESCE(tbl."foo" NOT LIKE '%__debezium_unavailable_value%', TRUE)`,
			RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Invalid)),
		)
	}
	{
		// Struct
		assert.Equal(t,
			`
COALESCE(
    CASE
        WHEN JSON_SIZE(tbl."foo") < 500 THEN JSON_SERIALIZE(tbl."foo") NOT LIKE '%__debezium_unavailable_value%'
    ELSE
        TRUE
    END,
    TRUE
)`, RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
		)
	}
	{
		// Array
		assert.Equal(t,
			`
COALESCE(
    CASE
        WHEN JSON_SIZE(tbl."foo") < 500 THEN JSON_SERIALIZE(tbl."foo") NOT LIKE '%__debezium_unavailable_value%'
    ELSE
        TRUE
    END,
    TRUE
)`, RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Array)),
		)
	}
	{
		// String
		assert.Equal(t,
			`COALESCE(tbl."foo" NOT LIKE '%__debezium_unavailable_value%', TRUE)`,
			RedshiftDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.String)),
		)
	}

}

func TestRedshiftDialect_BuildMergeInsertQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")
	assert.Equal(t,
		`INSERT INTO {TABLE_ID} ("col1","col2","col3") SELECT stg."col1",stg."col2",stg."col3" FROM {SUB_QUERY} AS stg LEFT JOIN {TABLE_ID} AS tgt ON tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" WHERE tgt."col1" IS NULL;`,
		RedshiftDialect{}.buildMergeInsertQuery(fakeTableID, "{SUB_QUERY}", []columns.Column{cols[0], cols[2]}, cols),
	)
}

func TestRedshiftDialect_BuildMergeUpdateQuery(t *testing.T) {
	testCases := []struct {
		name       string
		softDelete bool
		expected   []string
	}{
		{
			name:       "soft delete enabled",
			softDelete: true,
			expected: []string{
				`UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
				`UPDATE {TABLE_ID} AS tgt SET "__artie_delete"=stg."__artie_delete" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			},
		},
		{
			name:       "soft delete disabled",
			softDelete: false,
			expected: []string{
				`UPDATE {TABLE_ID} AS tgt SET "col1"=stg."col1","col2"=stg."col2","col3"=stg."col3" FROM {SUB_QUERY} AS stg WHERE tgt."col1" = stg."col1" AND tgt."col3" = stg."col3" AND COALESCE(stg."__artie_delete", false) = false;`,
			},
		},
	}

	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")

	for _, testCase := range testCases {
		actual := RedshiftDialect{}.buildMergeUpdateQueries(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[2]},
			cols,
			testCase.softDelete,
		)
		assert.Equal(t, testCase.expected, actual, testCase.name)
	}
}

func TestRedshiftDialect_BuildMergeDeleteQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE_ID}")
	assert.Equal(t,
		`DELETE FROM {TABLE_ID} WHERE ("col1","col2") IN (SELECT stg."col1",stg."col2" FROM {SUB_QUERY} AS stg WHERE stg."__artie_delete" = true);`,
		RedshiftDialect{}.buildMergeDeleteQuery(
			fakeTableID,
			"{SUB_QUERY}",
			[]columns.Column{cols[0], cols[1]},
		),
	)
}

type result struct {
	PrimaryKeys []columns.Column
	Columns     []columns.Column
}

// getBasicColumnsForTest - will return you all the columns within `result` that are needed for tests.
// * In here, we'll return if compositeKey=false - id (pk), email, first_name, last_name, created_at, toast_text (TOAST-able)
// * Else if compositeKey=true - id(pk), email (pk), first_name, last_name, created_at, toast_text (TOAST-able)
func getBasicColumnsForTest(compositeKey bool) result {
	idCol := columns.NewColumn("id", typing.Float)
	emailCol := columns.NewColumn("email", typing.String)
	textToastCol := columns.NewColumn("toast_text", typing.String)
	textToastCol.ToastColumn = true

	var cols columns.Columns
	cols.AddColumn(idCol)
	cols.AddColumn(emailCol)
	cols.AddColumn(columns.NewColumn("first_name", typing.String))
	cols.AddColumn(columns.NewColumn("last_name", typing.String))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampNTZ))
	cols.AddColumn(textToastCol)
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	var pks []columns.Column
	pks = append(pks, idCol)

	if compositeKey {
		pks = append(pks, emailCol)
	}

	return result{
		PrimaryKeys: pks,
		Columns:     cols.ValidColumns(),
	}
}

func TestRedshiftDialect_BuildMergeQueries_SkipDelete(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	parts, err := RedshiftDialect{}.BuildMergeQueries(
		fakeTableID,
		tempTableName,
		res.PrimaryKeys,
		nil,
		res.Columns,
		false,
		false,
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(parts))

	assert.Equal(t,
		`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_delete", false) = false;`,
		parts[0])

	assert.Equal(t,
		`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
		parts[1])
}

func TestRedshiftDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")

	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			parts[1])
		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[2])
	}
}

func TestRedshiftDialect_BuildMergeQueries_SoftDeleteComposite(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			res.PrimaryKeys,
			nil,
			res.Columns,
			true,
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."toast_text" ELSE tgt."toast_text" END,"__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
			parts[0])
		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "__artie_delete"=stg."__artie_delete" FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
			parts[1])
		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text","__artie_delete") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text",stg."__artie_delete" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[2])
	}
}

func TestRedshiftDialect_BuildMergeQueries(t *testing.T) {
	// Biggest difference with this test are:
	// 1. We are not saving `__artie_deleted` column
	// 2. There are 3 SQL queries (INSERT, UPDATE and DELETE)
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(false)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[0])

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id") IN (SELECT stg."id" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
}

func TestRedshiftDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	tempTableName := "public.tableName__temp"
	res := getBasicColumnsForTest(true)
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")
	{
		parts, err := RedshiftDialect{}.BuildMergeQueries(
			fakeTableID,
			tempTableName,
			res.PrimaryKeys,
			nil,
			res.Columns,
			false,
			true,
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(parts))

		assert.Equal(t,
			`UPDATE public.tableName AS tgt SET "id"=stg."id","email"=stg."email","first_name"=stg."first_name","last_name"=stg."last_name","created_at"=stg."created_at","toast_text"= CASE WHEN COALESCE(stg."toast_text" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."toast_text" ELSE tgt."toast_text" END FROM public.tableName__temp AS stg WHERE tgt."id" = stg."id" AND tgt."email" = stg."email" AND COALESCE(stg."__artie_delete", false) = false;`,
			parts[0])

		assert.Equal(t,
			`INSERT INTO public.tableName ("id","email","first_name","last_name","created_at","toast_text") SELECT stg."id",stg."email",stg."first_name",stg."last_name",stg."created_at",stg."toast_text" FROM public.tableName__temp AS stg LEFT JOIN public.tableName AS tgt ON tgt."id" = stg."id" AND tgt."email" = stg."email" WHERE tgt."id" IS NULL;`,
			parts[1])

		assert.Equal(t,
			`DELETE FROM public.tableName WHERE ("id","email") IN (SELECT stg."id",stg."email" FROM public.tableName__temp AS stg WHERE stg."__artie_delete" = true);`,
			parts[2])
	}
}

func TestRedshiftDialect_BuildIncreaseStringPrecisionQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{PUBLIC}.{TABLE}")
	assert.Equal(t,
		`ALTER TABLE {PUBLIC}.{TABLE} ALTER COLUMN "bar" TYPE VARCHAR(5)`,
		RedshiftDialect{}.BuildIncreaseStringPrecisionQuery(fakeTableID, "bar", 5),
	)
}

func TestRedshiftDialect_BuildCopyStatement(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("public.tableName")

	cols := []string{"id", "email", "first_name"}
	s3URI := "{{s3_uri}}"
	credentialsClause := "{{credentials}}"

	assert.Equal(t,
		fmt.Sprintf(`COPY public.tableName ("id","email","first_name") FROM '{{s3_uri}}' DELIMITER '\t' NULL AS '%s' GZIP FORMAT CSV %s dateformat 'auto' timeformat 'auto';`, constants.NullValuePlaceholder, credentialsClause),
		RedshiftDialect{}.BuildCopyStatement(fakeTableID, cols, s3URI, credentialsClause),
	)
}
