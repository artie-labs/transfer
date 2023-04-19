package snowflake

import (
	"fmt"
	"time"

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
	columns := map[string]typing.KindDetails{
		"id":                         typing.String,
		"first_name":                 typing.String,
		"invalid_column":             typing.Invalid,
		constants.DeleteColumnMarker: typing.Boolean,
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

	tableData := &optimization.TableData{
		InMemoryColumns: columns,
		RowsData:        rowsData,
		TopicConfig:     topicConfig,
		PrimaryKeys:     []string{"id"},
		Rows:            1,
	}

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), types.NewDwhTableConfig(
		map[string]typing.KindDetails{
			"id":                         typing.String,
			"first_name":                 typing.String,
			constants.DeleteColumnMarker: typing.Boolean,
		}, nil, false, true))

	err := s.store.Merge(s.ctx, tableData)
	assert.Equal(s.T(), tableData.InMemoryColumns["first_name"], typing.String)
	assert.NoError(s.T(), err)
}

func (s *SnowflakeTestSuite) TestExecuteMergeReestablishAuth() {
	columns := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(time.Now().Format(time.RFC3339Nano)),
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

	tableData := &optimization.TableData{
		InMemoryColumns: columns,
		RowsData:        rowsData,
		TopicConfig:     topicConfig,
		PrimaryKeys:     []string{"id"},
		Rows:            1,
	}

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake),
		types.NewDwhTableConfig(columns, nil, false, true))

	s.fakeStore.ExecReturnsOnCall(0, nil, fmt.Errorf("390114: Authentication token has expired. The user must authenticate again."))
	err := s.store.Merge(s.ctx, tableData)
	assert.True(s.T(), AuthenticationExpirationErr(err), err)

	s.fakeStore.ExecReturnsOnCall(1, nil, nil)
	assert.Nil(s.T(), s.store.Merge(s.ctx, tableData))
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 2, "called merge")
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	columns := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(time.Now().Format(time.RFC3339Nano)),
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

	tableData := &optimization.TableData{
		InMemoryColumns: columns,
		RowsData:        rowsData,
		TopicConfig:     topicConfig,
		PrimaryKeys:     []string{"id"},
		Rows:            1,
	}

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake),
		types.NewDwhTableConfig(columns, nil, false, true))
	err := s.store.Merge(s.ctx, tableData)
	assert.Nil(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 1, "called merge")
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

	defer s.store.configMap.RemoveTableFromConfig(topicConfig.ToFqName(constants.Snowflake))
	rowsData := make(map[string]map[string]interface{})
	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
			"id":         fmt.Sprintf("pk-%d", i),
			"created_at": time.Now().Format(time.RFC3339Nano),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	columns := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
		// Add kindDetails to created_at
		"created_at": typing.ParseValue(time.Now().Format(time.RFC3339Nano)),
	}

	tableData := &optimization.TableData{
		InMemoryColumns: columns,
		RowsData:        rowsData,
		TopicConfig:     topicConfig,
		PrimaryKeys:     []string{"id"},
		Rows:            1,
	}

	sflkColumns := map[string]typing.KindDetails{
		"id":                         typing.Integer,
		"created_at":                 typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"name":                       typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	sflkColumns["new"] = typing.String
	config := types.NewDwhTableConfig(sflkColumns, nil, false, true)
	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(constants.Snowflake), config)

	err := s.store.Merge(s.ctx, tableData)
	assert.Nil(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 1, "called merge")

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete()), 1,
		s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete())

	_, isOk := s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete()["new"]
	assert.True(s.T(), isOk)

	// Now try to execute merge where 1 of the rows have the column now
	for _, pkMap := range tableData.RowsData {
		pkMap["new"] = "123"
		tableData.InMemoryColumns = sflkColumns

		// Since sflkColumns overwrote the format, let's set it correctly again.
		tableData.InMemoryColumns["created_at"] = typing.ParseValue(time.Now().Format(time.RFC3339Nano))
		break
	}

	err = s.store.Merge(s.ctx, tableData)
	assert.NoError(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 2, "called merge again")

	// Caught up now, so columns should be 0.
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete()), 0,
		s.store.configMap.TableConfig(topicConfig.ToFqName(constants.Snowflake)).ColumnsToDelete())
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	err := s.store.Merge(s.ctx, &optimization.TableData{
		InMemoryColumns:         nil,
		RowsData:                nil,
		TopicConfig:             kafkalib.TopicConfig{},
		PartitionsToLastMessage: nil,
		LatestCDCTs:             time.Time{},
		Rows:                    0,
	})

	assert.Nil(s.T(), err)
}
