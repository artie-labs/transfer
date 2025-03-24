package snowflake

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func retrieveTableNameFromCreateTable(t *testing.T, query string) string {
	parts := strings.Split(query, ".")
	assert.Len(t, parts, 3)

	tableNamePart := parts[2]
	tableNameParts := strings.Split(tableNamePart, " ")
	assert.True(t, len(tableNameParts) > 2, tableNamePart)
	return strings.ReplaceAll(tableNameParts[0], `"`, "")
}

func (s *SnowflakeTestSuite) identifierFor(tableData *optimization.TableData) sql.TableIdentifier {
	return s.stageStore.IdentifierFor(tableData.TopicConfig(), tableData.Name())
}

func (s *SnowflakeTestSuite) TestDropTable() {
	tableData := optimization.NewTableData(nil, config.Replication, []string{"id"}, kafkalib.TopicConfig{Database: "customer", Schema: "public"}, fmt.Sprintf("%s_foo", constants.ArtiePrefix))
	tableID := s.identifierFor(tableData)
	{
		// Deleting without disabling drop protection
		assert.ErrorContains(s.T(), s.stageStore.DropTable(s.T().Context(), tableID), "not allowed to be dropped")
	}
	{
		// Deleting with disabling drop protection
		snowflakeTableID, ok := tableID.(dialect.TableIdentifier)
		assert.True(s.T(), ok)

		snowflakeTableID = snowflakeTableID.WithDisableDropProtection(true)
		assert.NoError(s.T(), s.stageStore.DropTable(s.T().Context(), snowflakeTableID))

		// Check store to see it drop
		_, query, _ := s.fakeStageStore.ExecContextArgsForCall(0)
		assert.Equal(s.T(), query, `DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."__ARTIE_FOO"`)
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
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.String,
		"first_name":                        typing.String,
		"invalid_column":                    typing.Invalid,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
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

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)

	_col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("first_name")
	assert.True(s.T(), isOk)
	assert.Equal(s.T(), _col.KindDetails, typing.String)
}

func (s *SnowflakeTestSuite) TestExecuteMergeReestablishAuth() {
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.MustParseValue("", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]any)

	for i := 0; i < 5; i++ {
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
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	assert.Equal(s.T(), 4, s.fakeStageStore.ExecCallCount())
	assert.Equal(s.T(), 1, s.fakeStageStore.ExecContextCallCount())
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	columnNames := []string{
		"id",
		"name",
		constants.DeleteColumnMarker,
		constants.OnlySetDeleteColumnMarker,
		"created_at",
	}

	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.MustParseValue("", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for _, colName := range columnNames {
		kd, ok := colToKindDetailsMap[colName]
		assert.True(s.T(), ok, colName)
		cols.AddColumn(columns.NewColumn(colName, kd))
	}

	rowsData := make(map[string]map[string]any)

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]any{
			"id":         i,
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
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
		tableData.InsertRow(pk, row, false)
	}

	tableID := s.identifierFor(tableData)
	fqName := tableID.FullyQualifiedName()
	s.stageStore.configMap.AddTable(tableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	s.fakeStageStore.ExecReturns(nil, nil)
	// CREATE TABLE IF NOT EXISTS customer.public.orders___artie_Mwv9YADmRy (id int,name string,__artie_delete boolean,created_at timestamp_tz) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='__artie_null_value' EMPTY_FIELD_AS_NULL=FALSE) COMMENT='expires:2023-06-27 11:54:03 UTC'
	_, createQuery, _ := s.fakeStageStore.ExecContextArgsForCall(0)
	tableName := retrieveTableNameFromCreateTable(s.T(), createQuery)
	assert.Contains(s.T(), createQuery, `"CUSTOMER"."PUBLIC"."ORDERS___ARTIE_`)

	// PUT 'file:///tmp/customer.public.orders___artie_Mwv9YADmRy.csv' @customer.public.%orders___artie_Mwv9YADmRy AUTO_COMPRESS=TRUE
	putQuery, _ := s.fakeStageStore.ExecArgsForCall(0)
	assert.Contains(s.T(), putQuery, "PUT 'file://")

	// COPY INTO "CUSTOMER"."PUBLIC"."ORDERS___ARTIE_Mwv9YADmRy" ("ID","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","CREATED_AT") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%orders___artie_Mwv9YADmRy" FILES = ('CUSTOMER.PUBLIC.orders___artie_Mwv9YADmRy.csv'))
	copyQuery, _ := s.fakeStageStore.ExecArgsForCall(1)
	expectedCopyQuery := fmt.Sprintf(`COPY INTO "CUSTOMER"."PUBLIC"."%s" ("ID","NAME","__ARTIE_DELETE","__ARTIE_ONLY_SET_DELETE","CREATED_AT") FROM (SELECT $1,$2,$3,$4,$5 FROM @"CUSTOMER"."PUBLIC"."%%%s" FILES = ('CUSTOMER.PUBLIC.%s.csv'))`, tableName, tableName, tableName)
	assert.Equal(s.T(), expectedCopyQuery, copyQuery)

	mergeQuery, _ := s.fakeStageStore.ExecArgsForCall(2)
	assert.Contains(s.T(), mergeQuery, fmt.Sprintf("MERGE INTO %s", fqName), fmt.Sprintf("query: %v, destKind: %v", mergeQuery, constants.Snowflake))

	// Drop a table now.
	dropQuery, _ := s.fakeStageStore.ExecArgsForCall(3)
	assert.Contains(s.T(), dropQuery, `DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."ORDERS___ARTIE_`,
		fmt.Sprintf("query: %v, destKind: %v", dropQuery, constants.Snowflake))

	assert.Equal(s.T(), 4, s.fakeStageStore.ExecCallCount())
	assert.Equal(s.T(), 1, s.fakeStageStore.ExecContextCallCount())
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

	rowsData := make(map[string]map[string]any)
	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]any{
			"id":         i,
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.TimestampTZ,
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, topicConfig, "foo")
	tableData.ResetTempTableSuffix()
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	snowflakeColToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"created_at":                        typing.TimestampTZ,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	}

	var sflkCols columns.Columns
	for colName, colKind := range snowflakeColToKindDetailsMap {
		sflkCols.AddColumn(columns.NewColumn(colName, colKind))
	}

	sflkCols.AddColumn(columns.NewColumn("new", typing.String))
	_config := types.NewDestinationTableConfig(sflkCols.GetColumns(), true)
	s.stageStore.configMap.AddTable(s.identifierFor(tableData), _config)

	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), 4, s.fakeStageStore.ExecCallCount())
	assert.Equal(s.T(), 1, s.fakeStageStore.ExecContextCallCount())

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()), 1,
		s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete())

	_, isOk := s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()["new"]
	assert.True(s.T(), isOk)

	// Now try to execute merge where 1 of the rows have the column now
	for _, row := range tableData.Rows() {
		row["new"] = "123"
		tableData.SetInMemoryColumns(&sflkCols)

		inMemColumns := tableData.ReadOnlyInMemoryCols()
		// Since sflkColumns overwrote the format, let's set it correctly again.
		inMemColumns.UpdateColumn(columns.NewColumn("created_at", typing.TimestampTZ))
		tableData.SetInMemoryColumns(inMemColumns)
		break
	}

	commitTx, err = s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), 8, s.fakeStageStore.ExecCallCount())
	assert.Equal(s.T(), 2, s.fakeStageStore.ExecContextCallCount())

	// Caught up now, so columns should be 0.
	assert.Len(s.T(), s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete(), 0)
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)
}

func (s *SnowflakeTestSuite) TestStore_AdditionalEqualityStrings() {
	{
		// No additional equality strings:
		tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
		assert.Empty(s.T(), s.stageStore.additionalEqualityStrings(tableData))
	}
	{
		// Additional equality strings:
		topicConfig := kafkalib.TopicConfig{}
		topicConfig.AdditionalMergePredicates = []partition.MergePredicates{
			{PartitionField: "foo"},
			{PartitionField: "bar"},
		}
		tableData := optimization.NewTableData(nil, config.Replication, nil, topicConfig, "foo")
		actual := s.stageStore.additionalEqualityStrings(tableData)
		assert.Equal(s.T(), []string{`tgt."FOO" = stg."FOO"`, `tgt."BAR" = stg."BAR"`}, actual)
	}
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
	tableID := (&Store{}).IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tempTableName := shared.TempTableIDWithSuffix(tableID, "sUfFiX").FullyQualifiedName()
	assert.Equal(t, `"DB"."SCHEMA"."TABLE___ARTIE_SUFFIX"`, trimTTL(tempTableName))
}
