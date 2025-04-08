package snowflake

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/stretchr/testify/assert"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *SnowflakeTestSuite) TestBuildRemoveFilesFromStage() {
	table := dialect.NewTableIdentifier("db", "schema", "table")

	query := s.stageStore.dialect().BuildRemoveFilesFromStage(addPrefixToTableName(table, "%"), "")
	assert.Equal(s.T(), `REMOVE @"DB"."SCHEMA"."%TABLE"`, query)
}

func (s *SnowflakeTestSuite) TestReplaceExceededValues() {
	// String + OptionalStringPrecision not set + equal to max LOB length:
	assert.Equal(s.T(), strings.Repeat("a", 16777216), replaceExceededValues(strings.Repeat("a", 16777216), typing.String))
	// String + OptionalStringPrecision not set + greater than max LOB length:
	assert.Equal(s.T(), constants.ExceededValueMarker, replaceExceededValues(strings.Repeat("a", 16777217), typing.String))
	// String + OptionalStringPrecision set + equal to OptionalStringPrecision:
	assert.Equal(s.T(),
		strings.Repeat("a", 100),
		replaceExceededValues(strings.Repeat("a", 100), typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(100))}),
	)
	// String + OptionalStringPrecision set + larger than OptionalStringPrecision:
	assert.Equal(s.T(),
		constants.ExceededValueMarker,
		replaceExceededValues(strings.Repeat("a", 101), typing.KindDetails{Kind: typing.String.Kind, OptionalStringPrecision: typing.ToPtr(int32(100))}),
	)
}

func (s *SnowflakeTestSuite) TestCastColValStaging() {
	{
		// Null
		value, err := castColValStaging(nil, typing.String)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), constants.NullValuePlaceholder, value)
	}
	{
		// Struct field

		// Did not exceed lob size
		value, err := castColValStaging(map[string]any{"key": "value"}, typing.Struct)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), `{"key":"value"}`, value)

		// Did exceed lob size
		value, err = castColValStaging(map[string]any{"key": strings.Repeat("a", 16777216)}, typing.Struct)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), `{"key":"__artie_exceeded_value"}`, value)
	}
	{
		// String field
		value, err := castColValStaging("foo", typing.String)
		assert.NoError(s.T(), err)
		assert.Equal(s.T(), "foo", value)
	}
}

// runTestCaseWithReset runs a test case with a fresh store state
func (s *SnowflakeTestSuite) runTestCaseWithReset(fn func()) {
	s.ResetStore()
	fn()
}

func (s *SnowflakeTestSuite) TestBackfillColumn() {
	tableID := dialect.NewTableIdentifier("db", "public", "tableName")

	needsBackfillCol := columns.NewColumn("foo", typing.Boolean)
	needsBackfillCol.SetDefaultValue(true)
	needsBackfillColDefault := columns.NewColumn("default", typing.Boolean)
	needsBackfillColDefault.SetDefaultValue(true)

	s.runTestCaseWithReset(func() {
		// col that doesn't have default value
		assert.NoError(s.T(), shared.BackfillColumn(s.T().Context(), s.stageStore, columns.NewColumn("foo", typing.Invalid), tableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	})

	s.runTestCaseWithReset(func() {
		// col that has default value but already backfilled
		backfilledCol := columns.NewColumn("foo", typing.Boolean)
		backfilledCol.SetDefaultValue(true)
		backfilledCol.SetBackfilled(true)
		assert.NoError(s.T(), shared.BackfillColumn(s.T().Context(), s.stageStore, backfilledCol, tableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	})

	s.runTestCaseWithReset(func() {
		// col that has default value that needs to be backfilled
		s.mockDB.ExpectExec(`UPDATE "DB"."PUBLIC"."TABLENAME" as t SET t."FOO" = true WHERE t."FOO" IS NULL;`).WillReturnResult(sqlmock.NewResult(0, 0))
		s.mockDB.ExpectExec(`COMMENT ON COLUMN "DB"."PUBLIC"."TABLENAME"."FOO" IS '{"backfilled": true}';`).WillReturnResult(sqlmock.NewResult(0, 0))

		assert.NoError(s.T(), shared.BackfillColumn(s.T().Context(), s.stageStore, needsBackfillCol, tableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	})

	s.runTestCaseWithReset(func() {
		// default col that has default value that needs to be backfilled
		s.mockDB.ExpectExec(`UPDATE "DB"."PUBLIC"."TABLENAME" as t SET t."DEFAULT" = true WHERE t."DEFAULT" IS NULL;`).WillReturnResult(sqlmock.NewResult(0, 0))
		s.mockDB.ExpectExec(`COMMENT ON COLUMN "DB"."PUBLIC"."TABLENAME"."DEFAULT" IS '{"backfilled": true}';`).WillReturnResult(sqlmock.NewResult(0, 0))
		assert.NoError(s.T(), shared.BackfillColumn(s.T().Context(), s.stageStore, needsBackfillColDefault, tableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	})

	s.runTestCaseWithReset(func() {
		// default col that has default value that needs to be backfilled (repeat)
		s.mockDB.ExpectExec(`UPDATE "DB"."PUBLIC"."TABLENAME" as t SET t."DEFAULT" = true WHERE t."DEFAULT" IS NULL;`).WillReturnResult(sqlmock.NewResult(0, 0))
		s.mockDB.ExpectExec(`COMMENT ON COLUMN "DB"."PUBLIC"."TABLENAME"."DEFAULT" IS '{"backfilled": true}';`).WillReturnResult(sqlmock.NewResult(0, 0))
		assert.NoError(s.T(), shared.BackfillColumn(s.T().Context(), s.stageStore, needsBackfillColDefault, tableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	})
}

// generateTableData - returns tableName and tableData
func generateTableData(rows int) (dialect.TableIdentifier, *optimization.TableData) {
	randomTableName := fmt.Sprintf("temp_%s_%s", constants.ArtiePrefix, stringutil.Random(10))
	cols := &columns.Columns{}
	for _, col := range []string{"user_id", "first_name", "last_name", "dusty"} {
		cols.AddColumn(columns.NewColumn(col, typing.String))
	}

	td := optimization.NewTableData(cols, config.Replication, []string{"user_id"}, kafkalib.TopicConfig{}, "")
	for i := 0; i < rows; i++ {
		key := fmt.Sprint(i)
		rowData := map[string]any{
			"user_id":    key,
			"first_name": fmt.Sprintf("first_name %d", i),
			"last_name":  fmt.Sprintf("last_name %d", i),
			"dusty":      "the mini aussie",
		}

		td.InsertRow(key, rowData, false)
	}

	return dialect.NewTableIdentifier("database", "schema", randomTableName), td
}

func (s *SnowflakeTestSuite) TestPrepareTempTable() {
	tempTableID, tableData := generateTableData(10)
	tempTableName := tempTableID.FullyQualifiedName()
	s.stageStore.GetConfigMap().AddTable(tempTableID, types.NewDestinationTableConfig(nil, true))
	sflkTc := s.stageStore.GetConfigMap().GetTableConfig(tempTableID)

	{
		// Set up expectations for the first test case
		// Use regexp.MustCompile to properly escape special characters
		createTableRegex := regexp.QuoteMeta(fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s ("USER_ID" string,"FIRST_NAME" string,"LAST_NAME" string,"DUSTY" string) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`, tempTableName))
		s.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

		stagingTableID := tempTableID.WithTable("%" + tempTableID.Table())
		putQueryRegex := regexp.QuoteMeta(fmt.Sprintf(`PUT 'file://%s' @"DATABASE"."SCHEMA".%s AUTO_COMPRESS=TRUE`,
			filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", strings.ReplaceAll(tempTableName, `"`, ""))),
			stagingTableID.EscapedTable()))
		s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

		resourceName := addPrefixToTableName(tempTableID, "%")
		copyQueryRegex := regexp.QuoteMeta(fmt.Sprintf(`COPY INTO %s ("USER_ID","FIRST_NAME","LAST_NAME","DUSTY") FROM (SELECT $1,$2,$3,$4 FROM @%s) FILES = ('%s.csv.gz')`,
			tempTableName, resourceName, strings.ReplaceAll(tempTableName, `"`, "")))
		s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

		assert.NoError(s.T(), s.stageStore.PrepareTemporaryTable(s.T().Context(), tableData, sflkTc, tempTableID, tempTableID, types.AdditionalSettings{}, true))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	}
	{
		// Set up expectations for the second test case (don't create temporary table)
		stagingTableID := tempTableID.WithTable("%" + tempTableID.Table())
		putQueryRegex := regexp.QuoteMeta(fmt.Sprintf(`PUT 'file://%s' @"DATABASE"."SCHEMA".%s AUTO_COMPRESS=TRUE`,
			filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", strings.ReplaceAll(tempTableName, `"`, ""))),
			stagingTableID.EscapedTable()))
		s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

		resourceName := addPrefixToTableName(tempTableID, "%")
		copyQueryRegex := regexp.QuoteMeta(fmt.Sprintf(`COPY INTO %s ("USER_ID","FIRST_NAME","LAST_NAME","DUSTY") FROM (SELECT $1,$2,$3,$4 FROM @%s) FILES = ('%s.csv.gz')`,
			tempTableName, resourceName, strings.ReplaceAll(tempTableName, `"`, "")))
		s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

		assert.NoError(s.T(), s.stageStore.PrepareTemporaryTable(s.T().Context(), tableData, sflkTc, tempTableID, tempTableID, types.AdditionalSettings{}, false))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
	}
}

func (s *SnowflakeTestSuite) TestLoadTemporaryTable() {
	tempTableID, tableData := generateTableData(100)
	file, err := s.stageStore.writeTemporaryTableFile(tableData, tempTableID)
	assert.Equal(s.T(), fmt.Sprintf("%s.csv", strings.ReplaceAll(tempTableID.FullyQualifiedName(), `"`, "")), file.FileName)
	assert.NoError(s.T(), err)
	// Read the CSV and confirm.
	csvfile, err := os.Open(file.FilePath)
	assert.NoError(s.T(), err)
	// Parse the file
	r := csv.NewReader(csvfile)
	r.Comma = '\t'

	seenUserID := make(map[string]bool)
	seenFirstName := make(map[string]bool)
	seenLastName := make(map[string]bool)
	// Iterate through the records
	for {
		// Read each record from csv
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}

		assert.NoError(s.T(), readErr)
		assert.Equal(s.T(), 4, len(record))

		// [user_id, first_name, last_name]
		seenUserID[record[0]] = true
		seenFirstName[record[1]] = true
		seenLastName[record[2]] = true
		assert.Equal(s.T(), "the mini aussie", record[3])
	}

	assert.Len(s.T(), seenUserID, int(tableData.NumberOfRows()))
	assert.Len(s.T(), seenFirstName, int(tableData.NumberOfRows()))
	assert.Len(s.T(), seenLastName, int(tableData.NumberOfRows()))

	// Delete the file.
	assert.NoError(s.T(), os.RemoveAll(file.FilePath))
}
