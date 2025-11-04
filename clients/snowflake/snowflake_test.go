package snowflake

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func buildBaseRow(data map[string]any) optimization.Row {
	data[constants.DeleteColumnMarker] = false
	data[constants.OnlySetDeleteColumnMarker] = false
	return optimization.NewRow(data)
}

func (s *SnowflakeTestSuite) identifierFor(tableData *optimization.TableData) sql.TableIdentifier {
	return s.stageStore.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
}

func (s *SnowflakeTestSuite) TestDropTable() {
	tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, kafkalib.TopicConfig{Database: "customer", Schema: "public"}, fmt.Sprintf("%s_foo", constants.ArtiePrefix))
	tableID := s.identifierFor(tableData)
	{
		// Deleting without disabling drop protection
		assert.ErrorContains(s.T(), s.stageStore.DropTable(s.T().Context(), tableID), "is not a temporary table")
	}
	{
		// Deleting with disabling drop protection
		snowflakeTableID, ok := tableID.(dialect.TableIdentifier)
		assert.True(s.T(), ok)

		snowflakeTableID = snowflakeTableID.WithTemporaryTable(true).(dialect.TableIdentifier)

		// Set up expectation for DROP TABLE query
		s.mockDB.ExpectExec(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."__ARTIE_FOO"`).WillReturnResult(sqlmock.NewResult(0, 0))

		assert.NoError(s.T(), s.stageStore.DropTable(s.T().Context(), snowflakeTableID))
		assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())

		// Cache should be empty as well.
		assert.Nil(s.T(), s.stageStore.configMap.GetTableConfig(snowflakeTableID))
	}
}

func (s *SnowflakeTestSuite) TestExecuteMergeNilEdgeCase() {
	// This test was written for https://github.com/artie-labs/transfer/pull/26
	// Say the column first_name already exists in Snowflake as "STRING"
	// I want to delete the value, so I update Postgres and set the cell to be null
	// TableData will think the column is invalid and tableConfig will think column = string
	// Before we call merge, it should reconcile it.
	colToKindDetailsMap := maputil.NewOrderedMap[typing.KindDetails](true)
	colToKindDetailsMap.Add("id", typing.String)
	colToKindDetailsMap.Add("first_name", typing.String)
	colToKindDetailsMap.Add("invalid_column", typing.Invalid)
	colToKindDetailsMap.Add(constants.DeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add(constants.OnlySetDeleteColumnMarker, typing.Boolean)

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap.All() {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := map[string]map[string]any{
		"pk-1": {
			"first_name": "bob",
		},
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, topicConfig, "foo")
	assert.Equal(s.T(), "foo", tableData.Name())

	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	anotherColToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.String,
		"first_name":                        typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	}

	var anotherCols []columns.Column
	for colName, kindDetails := range anotherColToKindDetailsMap {
		anotherCols = append(anotherCols, columns.NewColumn(colName, kindDetails))
	}

	s.stageStore.configMap.AddTable(s.identifierFor(tableData), types.NewDestinationTableConfig(anotherCols, true))

	// Set up expectations for CREATE TABLE - use regex pattern to match the actual table name with suffix
	createTableRegex := regexp.QuoteMeta(`CREATE TRANSIENT TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID" string,"FIRST_NAME" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	s.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for PUT - use regex pattern to match the actual table name with suffix
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%`) + `.*"`
	s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for COPY INTO - use regex pattern to match the actual table name with suffix
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID","FIRST_NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE") FROM (SELECT $1,$2,$3,$4 FROM @"CUSTOMER"."PUBLIC"."%`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

	// Set up expectations for MERGE - use regex pattern to match the actual table name with suffix
	mergeQueryRegex := regexp.QuoteMeta(`MERGE INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(mergeQueryRegex).WillReturnResult(sqlmock.NewResult(0, int64(len(rowsData))))

	// Set up expectations for DROP TABLE - use regex pattern to match the actual table name with suffix
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())

	_col, ok := tableData.ReadOnlyInMemoryCols().GetColumn("first_name")
	assert.True(s.T(), ok)
	assert.Equal(s.T(), _col.KindDetails, typing.String)
}

func (s *SnowflakeTestSuite) TestExecuteMergeReestablishAuth() {
	colToKindDetailsMap := maputil.NewOrderedMap[typing.KindDetails](true)
	colToKindDetailsMap.Add("id", typing.Integer)
	colToKindDetailsMap.Add("name", typing.String)
	colToKindDetailsMap.Add(constants.DeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add(constants.OnlySetDeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add("created_at", typing.MustParseValue("", nil, time.Now().Format(time.RFC3339Nano)))

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap.All() {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]any)
	for i := range 5 {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]any{
			"id":         i,
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, topicConfig, "foo")
	tableData.ResetTempTableSuffix()
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	s.stageStore.configMap.AddTable(s.identifierFor(tableData), types.NewDestinationTableConfig(cols.GetColumns(), true))

	// Set up expectations for CREATE TABLE - use regex pattern to match the actual table name with suffix
	createTableRegex := regexp.QuoteMeta(`CREATE TRANSIENT TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID" int,"NAME" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean,"CREATED_AT" string) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	s.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for PUT - use regex pattern to match the actual table name with suffix
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%`) + `.*"`
	s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for COPY INTO - use regex pattern to match the actual table name with suffix
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","CREATED_AT") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

	// Set up expectations for MERGE - use regex pattern to match the actual table name with suffix
	mergeQueryRegex := regexp.QuoteMeta(`MERGE INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(mergeQueryRegex).WillReturnResult(sqlmock.NewResult(0, int64(len(rowsData))))

	// Set up expectations for DROP TABLE - use regex pattern to match the actual table name with suffix
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	colToKindDetailsMap := maputil.NewOrderedMap[typing.KindDetails](true)
	colToKindDetailsMap.Add("id", typing.Integer)
	colToKindDetailsMap.Add("name", typing.String)
	colToKindDetailsMap.Add(constants.DeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add(constants.OnlySetDeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add("created_at", typing.MustParseValue("", nil, time.Now().Format(time.RFC3339Nano)))
	var cols columns.Columns
	for colName, kindDetails := range colToKindDetailsMap.All() {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	rowsData := make(map[string]optimization.Row)
	for i := range 5 {
		rowsData[fmt.Sprintf("pk-%d", i)] = buildBaseRow(map[string]any{
			"id":         i,
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		})
	}

	tblName := "orders"
	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: tblName,
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, topicConfig, tblName)
	tableData.ResetTempTableSuffix()
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row.GetData(), false)
	}

	tableID := s.identifierFor(tableData)
	s.stageStore.configMap.AddTable(tableID, types.NewDestinationTableConfig(cols.GetColumns(), true))

	// Set up expectations for CREATE TABLE - use regex pattern to match the actual table name with suffix
	createTableRegex := regexp.QuoteMeta(`CREATE TRANSIENT TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID" int,"NAME" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean,"CREATED_AT" string) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	s.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for PUT - use regex pattern to match the actual table name with suffix
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%`) + `.*"`
	s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for COPY INTO - use regex pattern to match the actual table name with suffix
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","CREATED_AT") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

	// Set up expectations for MERGE - use regex pattern to match the actual table name with suffix
	mergeQueryRegex := regexp.QuoteMeta(`MERGE INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(mergeQueryRegex).WillReturnResult(sqlmock.NewResult(0, int64(len(rowsData))))

	// Set up expectations for DROP TABLE - use regex pattern to match the actual table name with suffix
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
}

// TestExecuteMergeDeletionFlagRemoval is going to run execute merge twice.
// First time, we will try to delete a column
// Second time, we'll simulate the data catching up (column exists) and it should now
// Remove it from the in-memory store.
func (s *SnowflakeTestSuite) TestExecuteMergeDeletionFlagRemoval() {
	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	rowsData := make(map[string]optimization.Row)
	for i := range 5 {
		rowsData[fmt.Sprintf("pk-%d", i)] = buildBaseRow(map[string]any{
			"id":         i,
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		})
	}

	colToKindDetailsMap := maputil.NewOrderedMap[typing.KindDetails](true)
	colToKindDetailsMap.Add("id", typing.Integer)
	colToKindDetailsMap.Add("name", typing.String)
	colToKindDetailsMap.Add(constants.DeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add(constants.OnlySetDeleteColumnMarker, typing.Boolean)
	colToKindDetailsMap.Add("created_at", typing.TimestampTZ)

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap.All() {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, topicConfig, "foo")
	tableData.ResetTempTableSuffix()
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row.GetData(), false)
	}

	snowflakeColToKindDetailsMap := maputil.NewOrderedMap[typing.KindDetails](true)
	snowflakeColToKindDetailsMap.Add("id", typing.Integer)
	snowflakeColToKindDetailsMap.Add("created_at", typing.TimestampTZ)
	snowflakeColToKindDetailsMap.Add("name", typing.String)
	snowflakeColToKindDetailsMap.Add(constants.DeleteColumnMarker, typing.Boolean)
	snowflakeColToKindDetailsMap.Add(constants.OnlySetDeleteColumnMarker, typing.Boolean)

	var sflkCols columns.Columns
	for colName, colKind := range snowflakeColToKindDetailsMap.All() {
		sflkCols.AddColumn(columns.NewColumn(colName, colKind))
	}

	sflkCols.AddColumn(columns.NewColumn("new", typing.String))
	_config := types.NewDestinationTableConfig(sflkCols.GetColumns(), true)
	s.stageStore.configMap.AddTable(s.identifierFor(tableData), _config)

	// First merge - Set up expectations for CREATE TABLE - use regex pattern to match the actual table name with suffix
	createTableRegex := regexp.QuoteMeta(`CREATE TRANSIENT TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID" int,"NAME" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean,"CREATED_AT" timestamp_tz) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	s.mockDB.ExpectExec(createTableRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for PUT - use regex pattern to match the actual table name with suffix
	putQueryRegex := regexp.QuoteMeta(`PUT 'file://`) + `.*` + regexp.QuoteMeta(`' @"CUSTOMER"."PUBLIC"."%`) + `.*"`
	s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for COPY INTO - use regex pattern to match the actual table name with suffix
	copyQueryRegex := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","CREATED_AT") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	s.mockDB.ExpectQuery(copyQueryRegex).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

	// Set up expectations for MERGE - use regex pattern to match the actual table name with suffix
	mergeQueryRegex := regexp.QuoteMeta(`MERGE INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(mergeQueryRegex).WillReturnResult(sqlmock.NewResult(0, int64(len(rowsData))))

	// Set up expectations for DROP TABLE - use regex pattern to match the actual table name with suffix
	dropQueryRegex := regexp.QuoteMeta(`DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`"`)
	s.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()), 1,
		s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete())

	_, ok := s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()["new"]
	assert.True(s.T(), ok)

	// Now try to execute merge where 1 of the rows have the column now
	for _, row := range tableData.Rows() {
		pk, ok := row.GetValue("id")
		assert.True(s.T(), ok)

		rowData := row.GetData()
		rowData["new"] = "123"
		tableData.InsertRow(fmt.Sprintf("pk-%v", pk), rowData, false)
		tableData.SetInMemoryColumns(&sflkCols)
		inMemColumns := tableData.ReadOnlyInMemoryCols()
		// Since sflkColumns overwrote the format, let's set it correctly again.
		inMemColumns.UpdateColumn(columns.NewColumn("created_at", typing.TimestampTZ))
		tableData.SetInMemoryColumns(inMemColumns)
		break
	}

	// Second merge - Set up expectations for CREATE TABLE - use regex pattern to match the actual table name with suffix
	createTableRegex2 := regexp.QuoteMeta(`CREATE TRANSIENT TABLE IF NOT EXISTS "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID" int,"CREATED_AT" timestamp_tz,"NAME" string,"__ARTIE_DELETE" boolean,"__ARTIE_ONLY_SET_DELETE" boolean,"NEW" string) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE)`)
	s.mockDB.ExpectExec(createTableRegex2).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for PUT - use regex pattern to match the actual table name with suffix
	s.mockDB.ExpectExec(putQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	// Set up expectations for COPY INTO - use regex pattern to match the actual table name with suffix
	copyQueryRegex2 := regexp.QuoteMeta(`COPY INTO "CUSTOMER"."PUBLIC"."`) + `.*` + regexp.QuoteMeta(`" ("ID","CREATED_AT","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","NEW") FROM (SELECT $1,$2,$3,$4,$5,$6 FROM @"CUSTOMER"."PUBLIC"."%`) + `.*` + regexp.QuoteMeta(`") FILES = ('CUSTOMER.PUBLIC.`) + `.*` + regexp.QuoteMeta(`.csv.gz')`)
	s.mockDB.ExpectQuery(copyQueryRegex2).WillReturnRows(sqlmock.NewRows([]string{"rows_loaded"}).AddRow(fmt.Sprintf("%d", tableData.NumberOfRows())))

	// Set up expectations for MERGE - use regex pattern to match the actual table name with suffix
	s.mockDB.ExpectExec(mergeQueryRegex).WillReturnResult(sqlmock.NewResult(0, int64(len(rowsData))))

	// Set up expectations for DROP TABLE - use regex pattern to match the actual table name with suffix
	s.mockDB.ExpectExec(dropQueryRegex).WillReturnResult(sqlmock.NewResult(0, 0))

	commitTx, err = s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())

	// Caught up now, so columns should be 0.
	assert.Len(s.T(), s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete(), 0)
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	// No SQL should be executed for empty table data
	assert.NoError(s.T(), s.mockDB.ExpectationsWereMet())
}

func TestTempTableIDWithSuffix(t *testing.T) {
	trimTTL := func(tableName string) string {
		lastUnderscore := strings.LastIndex(tableName, "_")
		assert.GreaterOrEqual(t, lastUnderscore, 0)
		epoch, err := strconv.ParseInt(tableName[lastUnderscore+1:len(tableName)-1], 10, 64)
		assert.NoError(t, err)
		assert.Greater(t, time.Unix(epoch, 0), time.Now().Add(5*time.Hour)) // default TTL is 6 hours from now
		return tableName[:lastUnderscore] + string(tableName[len(tableName)-1])
	}

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{Database: "db", Schema: "schema"}, "table")
	tableID := (&Store{}).IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	tempTableName := shared.TempTableIDWithSuffix(tableID, "sUfFiX").FullyQualifiedName()
	assert.Equal(t, `"DB"."SCHEMA"."TABLE___ARTIE_SUFFIX"`, trimTTL(tempTableName))
}
