package dialect

import (
	"fmt"
	"slices"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestSnowflakeDialect_QuoteIdentifier(t *testing.T) {
	dialect := SnowflakeDialect{}
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
	assert.Equal(t, `"FOO; BAD"`, dialect.QuoteIdentifier(`FOO"; BAD`))
}

func TestSnowflakeDialect_IsTableDoesNotExistErr(t *testing.T) {
	errToExpectation := map[error]bool{
		nil: false,
		fmt.Errorf("Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized"): true,
		fmt.Errorf("hi this is super random"):                                        false,
	}

	for err, expectation := range errToExpectation {
		assert.Equal(t, SnowflakeDialect{}.IsTableDoesNotExistErr(err), expectation, err)
	}
}

func TestSnowflakeDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	// Temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2}) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`,
		SnowflakeDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1},{PART_2})`,
		SnowflakeDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
	)
}

func TestSnowflakeDialect_BuildDropTableQuery(t *testing.T) {
	assert.Equal(t,
		`DROP TABLE IF EXISTS "DATABASE1"."SCHEMA1"."TABLE1"`,
		SnowflakeDialect{}.BuildDropTableQuery(NewTableIdentifier("database1", "schema1", "table1")),
	)
}

func TestSnowflakeDialect_BuildTruncateTableQuery(t *testing.T) {
	assert.Equal(t,
		`TRUNCATE TABLE IF EXISTS "DATABASE1"."SCHEMA1"."TABLE1"`,
		SnowflakeDialect{}.BuildTruncateTableQuery(NewTableIdentifier("database1", "schema1", "table1")),
	)
}

func TestSnowflakeDialect_BuildAddColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS {SQL_PART}",
		SnowflakeDialect{}.BuildAddColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestSnowflakeDialect_BuildDropColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	assert.Equal(t,
		"ALTER TABLE {TABLE} DROP COLUMN IF EXISTS {SQL_PART}",
		SnowflakeDialect{}.BuildDropColumnQuery(fakeTableID, "{SQL_PART}"),
	)
}

func TestSnowflakeDialect_BuildIsNotToastValueExpression(t *testing.T) {
	{
		// Unspecified data type
		assert.Equal(t,
			`COALESCE(TO_VARCHAR(tbl."BAR") NOT LIKE '%__debezium_unavailable_value%', TRUE)`,
			SnowflakeDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.Invalid)),
		)
	}
	{
		// Structs
		assert.Equal(t,
			`COALESCE(TO_VARCHAR(tbl."FOO") NOT LIKE '%__debezium_unavailable_value%', TRUE)`,
			SnowflakeDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("foo", typing.Struct)),
		)
	}
	{
		// String
		assert.Equal(t,
			`COALESCE(tbl."BAR" NOT LIKE '%__debezium_unavailable_value%', TRUE)`,
			SnowflakeDialect{}.BuildIsNotToastValueExpression("tbl", columns.NewColumn("bar", typing.String)),
		)
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

func TestSnowflakeDialect_BuildMergeQueries_SoftDelete(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"bar":                               typing.String,
		"updated_at":                        typing.TimestampNTZ,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	{
		statements, err := SnowflakeDialect{}.BuildMergeQueries(
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
		assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND IFNULL(stg."__ARTIE_ONLY_SET_DELETE", false) = false THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE","BAR"=stg."BAR","ID"=stg."ID","UPDATED_AT"=stg."UPDATED_AT"
WHEN MATCHED AND IFNULL(stg."__ARTIE_ONLY_SET_DELETE", false) = true THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE"
WHEN NOT MATCHED THEN INSERT ("__ARTIE_DELETE","BAR","ID","UPDATED_AT") VALUES (stg."__ARTIE_DELETE",stg."BAR",stg."ID",stg."UPDATED_AT");`, statements[0])
	}
}

func TestSnowflakeDialect_BuildMergeQueryIntoStagingTable(t *testing.T) {
	fqTable := "db.schema.table"
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	{
		// Normal case
		_cols := buildColumns(map[string]typing.KindDetails{
			"id":                                typing.String,
			"bar":                               typing.String,
			"updated_at":                        typing.TimestampNTZ,
			constants.DeleteColumnMarker:        typing.Boolean,
			constants.OnlySetDeleteColumnMarker: typing.Boolean,
		})

		statements := SnowflakeDialect{}.BuildMergeQueryIntoStagingTable(
			fakeTableID,
			fqTable,
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
		)

		assert.Len(t, statements, 1)
		assert.Equal(t, `
MERGE INTO db.schema.table tgt USING ( db.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE"=stg."__ARTIE_ONLY_SET_DELETE","BAR"=stg."BAR","ID"=stg."ID","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED THEN INSERT ("__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","BAR","ID","UPDATED_AT") VALUES (stg."__ARTIE_DELETE",stg."__ARTIE_ONLY_SET_DELETE",stg."BAR",stg."ID",stg."UPDATED_AT");`, statements[0])
	}
	{
		// bar is toasted
		_cols := buildColumns(map[string]typing.KindDetails{
			"id":                                typing.String,
			"bar":                               typing.String,
			"updated_at":                        typing.TimestampNTZ,
			constants.DeleteColumnMarker:        typing.Boolean,
			constants.OnlySetDeleteColumnMarker: typing.Boolean,
		})

		_cols.UpsertColumn("bar", columns.UpsertColumnArg{
			ToastCol: typing.ToPtr(true),
		})

		statements := SnowflakeDialect{}.BuildMergeQueryIntoStagingTable(
			fakeTableID,
			fqTable,
			[]columns.Column{columns.NewColumn("id", typing.Invalid)},
			nil,
			_cols.ValidColumns(),
		)

		assert.Len(t, statements, 1)
		assert.Equal(t, `
MERGE INTO db.schema.table tgt USING ( db.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED THEN UPDATE SET "__ARTIE_DELETE"=stg."__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE"=stg."__ARTIE_ONLY_SET_DELETE","BAR"= CASE WHEN COALESCE(stg."BAR" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."BAR" ELSE tgt."BAR" END,"ID"=stg."ID","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED THEN INSERT ("__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","BAR","ID","UPDATED_AT") VALUES (stg."__ARTIE_DELETE",stg."__ARTIE_ONLY_SET_DELETE",stg."BAR",stg."ID",stg."UPDATED_AT");`, statements[0])
	}
}

func TestSnowflakeDialect_BuildMergeQueries(t *testing.T) {
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

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
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
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "BAR"=stg."BAR","ID"=stg."ID","START"=stg."START","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("BAR","ID","START","UPDATED_AT") VALUES (stg."BAR",stg."ID",stg."START",stg."UPDATED_AT");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_CompositeKey(t *testing.T) {
	fqTable := "database.schema.table"
	_cols := buildColumns(map[string]typing.KindDetails{
		"id":                                typing.String,
		"another_id":                        typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
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
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID" AND tgt."ANOTHER_ID" = stg."ANOTHER_ID"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "ANOTHER_ID"=stg."ANOTHER_ID","ID"=stg."ID"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("ANOTHER_ID","ID") VALUES (stg."ANOTHER_ID",stg."ID");`, statements[0])
}

func TestSnowflakeDialect_BuildMergeQueries_EscapePrimaryKeys(t *testing.T) {
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

	statements, err := SnowflakeDialect{}.BuildMergeQueries(
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
	assert.Equal(t, `
MERGE INTO database.schema.table tgt USING ( database.schema.table ) AS stg ON tgt."ID" = stg."ID" AND tgt."GROUP" = stg."GROUP"
WHEN MATCHED AND stg."__ARTIE_DELETE" THEN DELETE
WHEN MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN UPDATE SET "GROUP"=stg."GROUP","ID"=stg."ID","START"=stg."START","UPDATED_AT"=stg."UPDATED_AT"
WHEN NOT MATCHED AND IFNULL(stg."__ARTIE_DELETE", false) = false THEN INSERT ("GROUP","ID","START","UPDATED_AT") VALUES (stg."GROUP",stg."ID",stg."START",stg."UPDATED_AT");`, statements[0])
}

func TestSnowflakeDialect_BuildRemoveAllFilesFromStage(t *testing.T) {
	{
		// Stage name only, no path
		assert.Equal(t,
			"REMOVE @STAGE_NAME",
			SnowflakeDialect{}.BuildRemoveFilesFromStage("STAGE_NAME", ""),
		)
	}
	{
		// Stage name and path
		assert.Equal(t,
			"REMOVE @STAGE_NAME/path1/subpath2",
			SnowflakeDialect{}.BuildRemoveFilesFromStage("STAGE_NAME", "path1/subpath2"),
		)
	}
}

func TestSnowflakeDialect_EscapeColumns(t *testing.T) {
	{
		// Test basic string columns
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		assert.Equal(t, "$1,$2", SnowflakeDialect{}.EscapeColumns(cols.GetColumns(), ","))
	}
	{
		// Test string columns with struct
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		assert.Equal(t, "$1,$2,PARSE_JSON($3)", SnowflakeDialect{}.EscapeColumns(cols.GetColumns(), ","))
	}
	{
		// Test string columns with struct and array
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		cols.AddColumn(columns.NewColumn("array", typing.Array))
		assert.Equal(t, "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4", SnowflakeDialect{}.EscapeColumns(cols.GetColumns(), ","))
	}
	{
		// Test with invalid columns mixed in
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		cols.AddColumn(columns.NewColumn("array", typing.Array))
		assert.Equal(t, "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4", SnowflakeDialect{}.EscapeColumns(cols.GetColumns(), ","))
	}
}

func TestSnowflakeDialect_BuildCopyIntoTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("database.schema.table")

	cols := buildColumns(map[string]typing.KindDetails{
		"id":         typing.String,
		"name":       typing.String,
		"data":       typing.Struct,
		"tags":       typing.Array,
		"created_at": typing.TimestampNTZ,
	})

	query := SnowflakeDialect{}.BuildCopyIntoTableQuery(
		fakeTableID,
		cols.ValidColumns(),
		"%table_stage",
		"data.csv.gz",
	)

	expected := `COPY INTO database.schema.table ("CREATED_AT","DATA","ID","NAME","TAGS") ` +
		`FROM (SELECT $1,PARSE_JSON($2),$3,$4,CAST(PARSE_JSON($5) AS ARRAY) AS $5 FROM @%table_stage) ` +
		`FILES = ('data.csv.gz')`

	assert.Equal(t, expected, query)
}
