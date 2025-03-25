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

	// Verify the sequence of queries
	execCalls := s.fakeStageStore.ExecCallCount()
	execContextCalls := s.fakeStageStore.ExecContextCallCount()
	assert.Equal(s.T(), 2, execCalls, "Expected 2 Exec calls")
	assert.Equal(s.T(), 3, execContextCalls, "Expected 3 ExecContext calls")

	// Get all queries in sequence
	queries := make([]string, execCalls+execContextCalls)
	for i := 0; i < execContextCalls; i++ {
		_, query, _ := s.fakeStageStore.ExecContextArgsForCall(i)
		queries[i] = query
	}
	for i := 0; i < execCalls; i++ {
		_, args := s.fakeStageStore.ExecArgsForCall(i)
		queries[execContextCalls+i] = args[0].(string)
	}

	// Verify CREATE TABLE query
	assert.Contains(s.T(), queries[0], `"CUSTOMER"."PUBLIC"."FOO___ARTIE_`, "Expected CREATE TABLE query")

	// Verify PUT query
	assert.Contains(s.T(), queries[1], "PUT file://", "Expected PUT query")

	// Verify COPY INTO query
	assert.Contains(s.T(), queries[2], `COPY INTO "CUSTOMER"."PUBLIC"."FOO___ARTIE_`, "Expected COPY INTO query")
	assert.Contains(s.T(), queries[2], `FROM @"CUSTOMER"."PUBLIC"."%FOO___ARTIE_`, "Expected FROM clause")

	// Verify MERGE query
	assert.Contains(s.T(), queries[3], `MERGE INTO "CUSTOMER"."PUBLIC"."FOO"`, "Expected MERGE query")

	// Verify DROP query
	assert.Contains(s.T(), queries[4], `DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."FOO___ARTIE_`, "Expected DROP query")
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
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

	// Verify the sequence of queries
	execCalls := s.fakeStageStore.ExecCallCount()
	execContextCalls := s.fakeStageStore.ExecContextCallCount()
	assert.Equal(s.T(), 2, execCalls, "Expected 2 Exec calls")
	assert.Equal(s.T(), 3, execContextCalls, "Expected 3 ExecContext calls")

	// Get all queries in sequence
	queries := make([]string, execCalls+execContextCalls)
	for i := 0; i < execContextCalls; i++ {
		_, query, _ := s.fakeStageStore.ExecContextArgsForCall(i)
		queries[i] = query
	}
	for i := 0; i < execCalls; i++ {
		_, args := s.fakeStageStore.ExecArgsForCall(i)
		queries[execContextCalls+i] = args[0].(string)
	}

	// Verify CREATE TABLE query
	assert.Contains(s.T(), queries[0], `"CUSTOMER"."PUBLIC"."ORDERS___ARTIE_`, "Expected CREATE TABLE query")

	// Verify PUT query
	assert.Contains(s.T(), queries[1], "PUT file://", "Expected PUT query")

	// Verify COPY INTO query
	assert.Contains(s.T(), queries[2], `COPY INTO "CUSTOMER"."PUBLIC"."ORDERS___ARTIE_`, "Expected COPY INTO query")
	assert.Contains(s.T(), queries[2], `FROM @"CUSTOMER"."PUBLIC"."%ORDERS___ARTIE_`, "Expected FROM clause")

	// Verify MERGE query
	assert.Contains(s.T(), queries[3], fmt.Sprintf("MERGE INTO %s", fqName), "Expected MERGE query")

	// Verify DROP query
	assert.Contains(s.T(), queries[4], `DROP TABLE IF EXISTS "CUSTOMER"."PUBLIC"."ORDERS___ARTIE_`, "Expected DROP query")
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

	// First merge - should mark column for deletion
	commitTx, err := s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)

	// Verify first merge queries
	execCalls := s.fakeStageStore.ExecCallCount()
	execContextCalls := s.fakeStageStore.ExecContextCallCount()
	assert.Equal(s.T(), 2, execCalls, "Expected 2 Exec calls for first merge")
	assert.Equal(s.T(), 3, execContextCalls, "Expected 3 ExecContext calls for first merge")

	// Check the temp deletion table now
	assert.Equal(s.T(), 1, len(s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()),
		"Expected one column marked for deletion")

	_, isOk := s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()["new"]
	assert.True(s.T(), isOk, "Expected 'new' column to be marked for deletion")

	// Reset mock call counts for second merge
	s.fakeStageStore.ExecReturns(nil, nil)
	s.fakeStageStore.ExecContextReturns(nil, nil)

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

	// Second merge - should remove column from deletion list
	commitTx, err = s.stageStore.Merge(s.T().Context(), tableData)
	assert.NoError(s.T(), err)
	assert.True(s.T(), commitTx)

	// Verify second merge queries
	execCalls = s.fakeStageStore.ExecCallCount()
	execContextCalls = s.fakeStageStore.ExecContextCallCount()
	assert.Equal(s.T(), 2, execCalls, "Expected 2 Exec calls for second merge")
	assert.Equal(s.T(), 3, execContextCalls, "Expected 3 ExecContext calls for second merge")

	// Verify column is no longer marked for deletion
	assert.Len(s.T(), s.stageStore.configMap.GetTableConfig(s.identifierFor(tableData)).ReadOnlyColumnsToDelete(), 0,
		"Expected no columns marked for deletion")
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
