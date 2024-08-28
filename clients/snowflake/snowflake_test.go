package snowflake

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (s *SnowflakeTestSuite) identifierFor(tableData *optimization.TableData) sql.TableIdentifier {
	return s.stageStore.IdentifierFor(tableData.TopicConfig(), tableData.Name())
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

	var anotherCols columns.Columns
	for colName, kindDetails := range anotherColToKindDetailsMap {
		anotherCols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	s.stageStore.configMap.AddTableToConfig(s.identifierFor(tableData), types.NewDwhTableConfig(&anotherCols, nil, false, true))

	err := s.stageStore.Merge(tableData)
	_col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("first_name")
	assert.True(s.T(), isOk)
	assert.Equal(s.T(), _col.SourceKindDetails, typing.String)
	assert.NoError(s.T(), err)
}

func (s *SnowflakeTestSuite) TestExecuteMergeReestablishAuth() {
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(typing.Settings{}, "", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]any)

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]any{
			"id":         fmt.Sprintf("pk-%d", i),
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

	s.stageStore.configMap.AddTableToConfig(s.identifierFor(tableData), types.NewDwhTableConfig(&cols, nil, false, true))

	s.fakeStageStore.ExecReturnsOnCall(0, nil, fmt.Errorf("390114: Authentication token has expired. The user must authenticate again."))
	err := s.stageStore.Merge(tableData)
	assert.NoError(s.T(), err, "transient errors like auth errors will be retried")

	// 5 regular ones and then 1 additional one to re-establish auth and another one for dropping the temporary table
	baseline := 5
	assert.Equal(s.T(), baseline+2, s.fakeStageStore.ExecCallCount(), "called merge")
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                                typing.Integer,
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(typing.Settings{}, "", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]any)

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]any{
			"id":         fmt.Sprintf("pk-%d", i),
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

	var idx int

	tableID := s.identifierFor(tableData)
	fqName := tableID.FullyQualifiedName()
	s.stageStore.configMap.AddTableToConfig(tableID, types.NewDwhTableConfig(&cols, nil, false, true))
	err := s.stageStore.Merge(tableData)
	assert.Nil(s.T(), err)
	s.fakeStageStore.ExecReturns(nil, nil)
	// CREATE TABLE IF NOT EXISTS customer.public.orders___artie_Mwv9YADmRy (id int,name string,__artie_delete boolean,created_at timestamp_tz) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) COMMENT='expires:2023-06-27 11:54:03 UTC'
	createQuery, _ := s.fakeStageStore.ExecArgsForCall(idx)
	assert.Contains(s.T(), createQuery, `customer.public."ORDERS___ARTIE_`, fmt.Sprintf("query: %v, destKind: %v", createQuery, constants.Snowflake))

	// PUT file:///tmp/customer.public.orders___artie_Mwv9YADmRy.csv @customer.public.%orders___artie_Mwv9YADmRy AUTO_COMPRESS=TRUE
	putQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 1)
	assert.Contains(s.T(), putQuery, "PUT file://")

	// COPY INTO customer.public.orders___artie_Mwv9YADmRy (id,name,__artie_delete,created_at) FROM (SELECT $1,$2,$3,$4 FROM @customer.public.%orders___artie_Mwv9YADmRy
	copyQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 2)
	assert.Contains(s.T(), copyQuery, `COPY INTO customer.public."ORDERS___ARTIE_`, fmt.Sprintf("query: %v, destKind: %v", copyQuery, constants.Snowflake))
	assert.Contains(s.T(), copyQuery, fmt.Sprintf("FROM %s", "@customer.public.\"%ORDERS___ARTIE_"), fmt.Sprintf("query: %v, destKind: %v", copyQuery, constants.Snowflake))

	mergeQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 3)
	assert.Contains(s.T(), mergeQuery, fmt.Sprintf("MERGE INTO %s", fqName), fmt.Sprintf("query: %v, destKind: %v", mergeQuery, constants.Snowflake))

	// Drop a table now.
	dropQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 4)
	assert.Contains(s.T(), dropQuery, `DROP TABLE IF EXISTS customer.public."ORDERS___ARTIE_`,
		fmt.Sprintf("query: %v, destKind: %v", dropQuery, constants.Snowflake))

	assert.Equal(s.T(), 5, s.fakeStageStore.ExecCallCount(), "called merge")
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
			"id":         fmt.Sprintf("pk-%d", i),
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
		"created_at": typing.ParseValue(typing.Settings{}, "", nil, time.Now().Format(time.RFC3339Nano)),
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
		"created_at":                        typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"name":                              typing.String,
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	}

	var sflkCols columns.Columns
	for colName, colKind := range snowflakeColToKindDetailsMap {
		sflkCols.AddColumn(columns.NewColumn(colName, colKind))
	}

	sflkCols.AddColumn(columns.NewColumn("new", typing.String))
	_config := types.NewDwhTableConfig(&sflkCols, nil, false, true)
	s.stageStore.configMap.AddTableToConfig(s.identifierFor(tableData), _config)

	err := s.stageStore.Merge(tableData)
	assert.Nil(s.T(), err)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStageStore.ExecCallCount(), 5, "called merge")

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfigCache(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()), 1,
		s.stageStore.configMap.TableConfigCache(s.identifierFor(tableData)).ReadOnlyColumnsToDelete())

	_, isOk := s.stageStore.configMap.TableConfigCache(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()["new"]
	assert.True(s.T(), isOk)

	// Now try to execute merge where 1 of the rows have the column now
	for _, row := range tableData.Rows() {
		row["new"] = "123"
		tableData.SetInMemoryColumns(&sflkCols)

		inMemColumns := tableData.ReadOnlyInMemoryCols()
		// Since sflkColumns overwrote the format, let's set it correctly again.
		inMemColumns.UpdateColumn(columns.NewColumn("created_at", typing.ParseValue(typing.Settings{}, "", nil, time.Now().Format(time.RFC3339Nano))))
		tableData.SetInMemoryColumns(inMemColumns)
		break
	}

	err = s.stageStore.Merge(tableData)
	assert.NoError(s.T(), err)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStageStore.ExecCallCount(), 10, "called merge again")

	// Caught up now, so columns should be 0.
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfigCache(s.identifierFor(tableData)).ReadOnlyColumnsToDelete()), 0,
		s.stageStore.configMap.TableConfigCache(s.identifierFor(tableData)).ReadOnlyColumnsToDelete())
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	err := s.stageStore.Merge(tableData)
	assert.Nil(s.T(), err)
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
	assert.Equal(t, `db.schema."TABLE___ARTIE_SUFFIX"`, trimTTL(tempTableName))
}
