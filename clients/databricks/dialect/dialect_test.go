package dialect

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func TestDatabricksDialect_QuoteIdentifier(t *testing.T) {
	dialect := DatabricksDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
	assert.Equal(t, "`FOO; BAD`", dialect.QuoteIdentifier("FOO`; BAD"))
}

func TestDatabricksDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	{
		// No error
		assert.False(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(nil))
	}
	{
		// Random error
		assert.False(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("random error")))
	}
	{
		// Valid
		assert.True(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("[FIELDS_ALREADY_EXISTS] Cannot add column, because `first_name` already exists]")))
	}
}

func TestDatabricksDialect_IsTableDoesNotExistErr(t *testing.T) {
	{
		// No error
		assert.False(t, DatabricksDialect{}.IsTableDoesNotExistErr(nil))
	}
	{
		// Random error
		assert.False(t, DatabricksDialect{}.IsTableDoesNotExistErr(fmt.Errorf("random error")))
	}
	{
		// Valid
		assert.True(t, DatabricksDialect{}.IsTableDoesNotExistErr(fmt.Errorf("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: `foo`]")))
	}
}

func TestDatabricks_BuildIsNotToastValueExpression(t *testing.T) {
	{
		// Unspecified data type
		assert.Equal(t,
			"COALESCE(CAST(tbl.`bar` AS STRING) NOT LIKE '%__debezium_unavailable_value%', TRUE)",
			DatabricksDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
		)
	}
	{
		// Structs
		assert.Equal(t,
			"COALESCE(CAST(tbl.`foo` AS STRING) NOT LIKE '%__debezium_unavailable_value%', TRUE)",
			DatabricksDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
		)
	}
	{
		// String
		assert.Equal(t,
			"COALESCE(tbl.`bar` NOT LIKE '%__debezium_unavailable_value%', TRUE)",
			DatabricksDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.String)),
		)
	}
}

func TestDatabricksDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	{
		// Temporary
		assert.Equal(t,
			`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1}, {PART_2})`,
			DatabricksDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
		)
	}
	{
		// Not temporary
		assert.Equal(t,
			`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1}, {PART_2})`,
			DatabricksDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
		)
	}
}

func TestDatabricksDialect_BuildDropTableQuery(t *testing.T) {
	assert.Equal(t,
		"DROP TABLE IF EXISTS `project1`.`dataset2`.`table3`",
		DatabricksDialect{}.BuildDropTableQuery(NewTableIdentifier("project1", "dataset2", "table3")),
	)
}

func TestDatabricksDialect_BuildTruncateTableQuery(t *testing.T) {
	assert.Equal(t,
		"TRUNCATE TABLE `project1`.`dataset2`.`table3`",
		DatabricksDialect{}.BuildTruncateTableQuery(NewTableIdentifier("project1", "dataset2", "table3")),
	)
}

func TestDatabricksDialect_BuildAddColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")
	assert.Equal(t, "ALTER TABLE {TABLE} ADD COLUMN {SQL_PART} {DATA_TYPE}", DatabricksDialect{}.BuildAddColumnQuery(fakeTableID, "{SQL_PART} {DATA_TYPE}"))
}

func TestDatabricksDialect_BuildDropColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")
	assert.Equal(t, "ALTER TABLE {TABLE} DROP COLUMN {SQL_PART}", DatabricksDialect{}.BuildDropColumnQuery(fakeTableID, "{SQL_PART}"))
}

func TestDatabricksDialect_BuildDedupeQueries(t *testing.T) {
	dialect := DatabricksDialect{}
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TARGET}")

	fakeStagingTableID := &mocks.FakeTableIdentifier{}
	fakeStagingTableID.FullyQualifiedNameReturns("{STAGING}")

	{
		// includeArtieUpdatedAt = true
		queries := dialect.BuildDedupeQueries(fakeTableID, fakeStagingTableID, []string{"id"}, true)
		assert.Len(t, queries, 3)
		assert.Equal(t,
			fmt.Sprintf("CREATE TABLE {STAGING} AS SELECT * FROM {TARGET} QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s ASC, %s ASC) = 2",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier(constants.UpdateColumnMarker),
			),
			queries[0])
		assert.Equal(t,
			fmt.Sprintf("DELETE FROM {TARGET} t1 WHERE EXISTS (SELECT * FROM {STAGING} t2 WHERE t1.%s = t2.%s)",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id"),
			),
			queries[1])
		assert.Equal(t, "INSERT INTO {TARGET} SELECT * FROM {STAGING}", queries[2])
	}
	{
		// includeArtieUpdatedAt = false
		queries := dialect.BuildDedupeQueries(fakeTableID, fakeStagingTableID, []string{"id"}, false)
		assert.Len(t, queries, 3)
		assert.Equal(t,
			fmt.Sprintf("CREATE TABLE {STAGING} AS SELECT * FROM {TARGET} QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s ASC) = 2",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id")),
			queries[0])
		assert.Equal(t,
			fmt.Sprintf("DELETE FROM {TARGET} t1 WHERE EXISTS (SELECT * FROM {STAGING} t2 WHERE t1.%s = t2.%s)",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id")),
			queries[1])
		assert.Equal(t, "INSERT INTO {TARGET} SELECT * FROM {STAGING}", queries[2])
	}
}

func buildColumns(colTypesMap map[string]typing.KindDetails) *columns.Columns {
	var colNames []string
	for colName := range colTypesMap {
		colNames = append(colNames, colName)
	}
	// Sort the column names alphabetically to ensure deterministic order
	slices.Sort(colNames)

	var cols columns.Columns
	for _, colName := range colNames {
		cols.AddColumn(columns.NewColumn(colName, colTypesMap[colName]))
	}

	return &cols
}

func TestDatabricksDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"bar":                               typing.String,
		"updated_at":                        typing.TimestampTZ,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	{
		statements, err := DatabricksDialect{}.BuildMergeQueries(
			fakeTableID,
			fqTable,
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
			true,
			false,
		)
		assert.Len(t, statements, 1)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{
				"MERGE INTO database.schema.table tgt USING database.schema.table stg ON tgt.`id` = stg.`id`",
				"WHEN MATCHED AND IFNULL(stg.`__artie_only_set_delete`, false) = false THEN UPDATE SET `__artie_delete`=stg.`__artie_delete`,`bar`=stg.`bar`,`id`=stg.`id`,`updated_at`=stg.`updated_at`",
				"WHEN MATCHED AND IFNULL(stg.`__artie_only_set_delete`, false) = true THEN UPDATE SET `__artie_delete`=stg.`__artie_delete`",
				"WHEN NOT MATCHED THEN INSERT (`__artie_delete`,`bar`,`id`,`updated_at`) VALUES (stg.`__artie_delete`,stg.`bar`,stg.`id`,stg.`updated_at`);",
			},
			strings.Split(strings.TrimSpace(statements[0]), "\n"))
	}
}

func TestDatabricksDialect_BuildMergeQueries(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"bar":                               typing.String,
		"updated_at":                        typing.String,
		"start":                             typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := DatabricksDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
		[]columns.Column{columns.NewColumn("id", typing.Invalid)},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	assert.NoError(t, err)
	assert.Equal(t,
		[]string{
			"MERGE INTO database.schema.table tgt USING database.schema.table stg ON tgt.`id` = stg.`id`",
			"WHEN MATCHED AND stg.`__artie_delete` THEN DELETE", "WHEN MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN UPDATE SET `bar`=stg.`bar`,`id`=stg.`id`,`start`=stg.`start`,`updated_at`=stg.`updated_at`",
			"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`bar`,`id`,`start`,`updated_at`) VALUES (stg.`bar`,stg.`id`,stg.`start`,stg.`updated_at`);",
		},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestDatabricksDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"another_id":                        typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := DatabricksDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
		[]columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("another_id", typing.Invalid),
		},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	assert.NoError(t, err)
	assert.Equal(t,
		[]string{
			"MERGE INTO database.schema.table tgt USING database.schema.table stg ON tgt.`id` = stg.`id` AND tgt.`another_id` = stg.`another_id`",
			"WHEN MATCHED AND stg.`__artie_delete` THEN DELETE", "WHEN MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN UPDATE SET `another_id`=stg.`another_id`,`id`=stg.`id`",
			"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`another_id`,`id`) VALUES (stg.`another_id`,stg.`id`);",
		},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestDatabricksDialect_BuildMergeQueries_EscapePrimaryKeys(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"group":                             typing.String,
		"updated_at":                        typing.String,
		"start":                             typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := DatabricksDialect{}.BuildMergeQueries(
		fakeTableID,
		fqTable,
		[]columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("group", typing.Invalid),
		},
		nil,
		_cols.ValidColumns(),
		false,
		false,
	)
	assert.Len(t, statements, 1)
	assert.NoError(t, err)
	assert.Equal(t,
		[]string{
			"MERGE INTO database.schema.table tgt USING database.schema.table stg ON tgt.`id` = stg.`id` AND tgt.`group` = stg.`group`",
			"WHEN MATCHED AND stg.`__artie_delete` THEN DELETE", "WHEN MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN UPDATE SET `group`=stg.`group`,`id`=stg.`id`,`start`=stg.`start`,`updated_at`=stg.`updated_at`",
			"WHEN NOT MATCHED AND IFNULL(stg.`__artie_delete`, false) = false THEN INSERT (`group`,`id`,`start`,`updated_at`) VALUES (stg.`group`,stg.`id`,stg.`start`,stg.`updated_at`);",
		},
		strings.Split(strings.TrimSpace(statements[0]), "\n"))
}

func TestDatabricksDialect_BuildCopyIntoQuery(t *testing.T) {
	dialect := DatabricksDialect{}
	tempTableID := &mocks.FakeTableIdentifier{}
	tempTableID.FullyQualifiedNameReturns("{{fq_name}}")

	assert.Equal(t, fmt.Sprintf(`
COPY INTO {{fq_name}}
BY POSITION
FROM (
    SELECT _c0, _c1 FROM 'dbfs:/path/to/file.csv.gz'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'escape' = '"',
    'delimiter' = '\t',
    'header' = 'false',
    'nullValue' = '%s',
    'multiLine' = 'true',
    'compression' = 'gzip'
);`, constants.NullValuePlaceholder), dialect.BuildCopyIntoQuery(tempTableID, []string{"_c0", "_c1"}, "dbfs:/path/to/file.csv.gz"))
}
