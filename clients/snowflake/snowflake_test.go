package snowflake

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (s *SnowflakeTestSuite) TestExecuteMergeNilEdgeCase() {
	// This test was written for https://github.com/artie-labs/transfer/pull/26
	// Say the column first_name already exists in Snowflake as "STRING"
	// I want to delete the value, so I update Postgres and set the cell to be null
	// TableData will think the column is invalid and tableConfig will think column = string
	// Before we call merge, it should reconcile it.
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.String,
		"first_name":                 typing.String,
		"invalid_column":             typing.Invalid,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := map[string]map[string]interface{}{
		"pk-1": {
			"first_name": "bob",
		},
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig, "foo")
	assert.Equal(s.T(), topicConfig.TableName, tableData.Name(s.ctx, nil), "override is working")

	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	anotherColToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.String,
		"first_name":                 typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	var anotherCols columns.Columns
	for colName, kindDetails := range anotherColToKindDetailsMap {
		anotherCols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	s.stageStore.configMap.AddTableToConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true),
		types.NewDwhTableConfig(&anotherCols, nil, false, true))

	err := s.stageStore.Merge(s.ctx, tableData)
	_col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("first_name")
	assert.True(s.T(), isOk)
	assert.Equal(s.T(), _col.KindDetails, typing.String)
	assert.NoError(s.T(), err)
}

func (s *SnowflakeTestSuite) TestExecuteMergeReestablishAuth() {
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(s.ctx, "", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]interface{})

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
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

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig, "foo")
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	s.stageStore.configMap.AddTableToConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true),
		types.NewDwhTableConfig(&cols, nil, false, true))

	s.fakeStageStore.ExecReturnsOnCall(0, nil, fmt.Errorf("390114: Authentication token has expired. The user must authenticate again."))
	err := s.stageStore.Merge(s.ctx, tableData)
	assert.True(s.T(), AuthenticationExpirationErr(err), err)

	s.fakeStageStore.ExecReturnsOnCall(1, nil, nil)
	assert.Nil(s.T(), s.stageStore.Merge(s.ctx, tableData))
	s.fakeStageStore.ExecReturns(nil, nil)

	// 5 regular ones and then 1 additional one to re-establish auth.
	assert.Equal(s.T(), s.fakeStageStore.ExecCallCount(), 6, "called merge")
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(s.ctx, "", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	rowsData := make(map[string]map[string]interface{})

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
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

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig, "foo")
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	var idx int
	for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.SnowflakeStages} {
		fqName := tableData.ToFqName(s.ctx, destKind, true)
		s.stageStore.configMap.AddTableToConfig(fqName, types.NewDwhTableConfig(&cols, nil, false, true))
		err := s.stageStore.Merge(s.ctx, tableData)
		assert.Nil(s.T(), err)
		s.fakeStageStore.ExecReturns(nil, nil)
		// CREATE TABLE IF NOT EXISTS customer.public.orders___artie_Mwv9YADmRy (id int,name string,__artie_delete boolean,created_at timestamp_tz) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) COMMENT='expires:2023-06-27 11:54:03 UTC'
		createQuery, _ := s.fakeStageStore.ExecArgsForCall(idx)
		assert.Contains(s.T(), createQuery, fmt.Sprintf("%s_%s", fqName, constants.ArtiePrefix), fmt.Sprintf("query: %v, destKind: %v", createQuery, destKind))

		// PUT file:///tmp/customer.public.orders___artie_Mwv9YADmRy.csv @customer.public.%orders___artie_Mwv9YADmRy AUTO_COMPRESS=TRUE
		putQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 1)
		assert.Contains(s.T(), putQuery, fmt.Sprintf("PUT file:///tmp/%s_%s", fqName, constants.ArtiePrefix), fmt.Sprintf("query: %v, destKind: %v", putQuery, destKind))

		// COPY INTO customer.public.orders___artie_Mwv9YADmRy (id,name,__artie_delete,created_at) FROM (SELECT $1,$2,$3,$4 FROM @customer.public.%orders___artie_Mwv9YADmRy
		copyQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 2)
		assert.Contains(s.T(), copyQuery, fmt.Sprintf("COPY INTO %s_%s", fqName, constants.ArtiePrefix), fmt.Sprintf("query: %v, destKind: %v", copyQuery, destKind))
		assert.Contains(s.T(), copyQuery, fmt.Sprintf("FROM %s", "@customer.public.%orders___artie"), fmt.Sprintf("query: %v, destKind: %v", copyQuery, destKind))

		mergeQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 3)
		assert.Contains(s.T(), mergeQuery, fmt.Sprintf("MERGE INTO %s", fqName), fmt.Sprintf("query: %v, destKind: %v", mergeQuery, destKind))

		// Drop a table now.
		dropQuery, _ := s.fakeStageStore.ExecArgsForCall(idx + 4)
		assert.Contains(s.T(), dropQuery, fmt.Sprintf("DROP TABLE IF EXISTS %s", fmt.Sprintf("%s_%s", fqName, constants.ArtiePrefix)),
			fmt.Sprintf("query: %v, destKind: %v", dropQuery, destKind))
		idx += 5
	}

	assert.Equal(s.T(), 10, s.fakeStageStore.ExecCallCount(), "called merge")
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

	rowsData := make(map[string]map[string]interface{})
	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
			"id":         fmt.Sprintf("pk-%d", i),
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	colToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(s.ctx, "", nil, time.Now().Format(time.RFC3339Nano)),
	}

	var cols columns.Columns
	for colName, colKind := range colToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	tableData := optimization.NewTableData(&cols, []string{"id"}, topicConfig, "foo")
	for pk, row := range rowsData {
		tableData.InsertRow(pk, row, false)
	}

	snowflakeColToKindDetailsMap := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"created_at":                 typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	var sflkCols columns.Columns
	for colName, colKind := range snowflakeColToKindDetailsMap {
		sflkCols.AddColumn(columns.NewColumn(colName, colKind))
	}

	sflkCols.AddColumn(columns.NewColumn("new", typing.String))
	config := types.NewDwhTableConfig(&sflkCols, nil, false, true)
	s.stageStore.configMap.AddTableToConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true), config)

	err := s.stageStore.Merge(s.ctx, tableData)
	assert.Nil(s.T(), err)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStageStore.ExecCallCount(), 5, "called merge")

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true)).ReadOnlyColumnsToDelete()), 1,
		s.stageStore.configMap.TableConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true)).ReadOnlyColumnsToDelete())

	_, isOk := s.stageStore.configMap.TableConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true)).ReadOnlyColumnsToDelete()["new"]
	assert.True(s.T(), isOk)

	// Now try to execute merge where 1 of the rows have the column now
	for _, pkMap := range tableData.RowsData() {
		pkMap["new"] = "123"
		tableData.SetInMemoryColumns(&sflkCols)

		inMemColumns := tableData.ReadOnlyInMemoryCols()
		// Since sflkColumns overwrote the format, let's set it correctly again.
		inMemColumns.UpdateColumn(columns.NewColumn("created_at", typing.ParseValue(s.ctx, "", nil, time.Now().Format(time.RFC3339Nano))))
		tableData.SetInMemoryColumns(inMemColumns)
		break
	}

	err = s.stageStore.Merge(s.ctx, tableData)
	assert.NoError(s.T(), err)
	s.fakeStageStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStageStore.ExecCallCount(), 10, "called merge again")

	// Caught up now, so columns should be 0.
	assert.Equal(s.T(), len(s.stageStore.configMap.TableConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true)).ReadOnlyColumnsToDelete()), 0,
		s.stageStore.configMap.TableConfig(tableData.ToFqName(s.ctx, constants.Snowflake, true)).ReadOnlyColumnsToDelete())
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	tableData := optimization.NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	err := s.stageStore.Merge(s.ctx, tableData)
	assert.Nil(s.T(), err)
}
