package snowflake

import (
	"context"
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *SnowflakeTestSuite) TestCreateTable() {
	cols := []typing.Column{
		{
			Name: "key",
			Kind: typing.String,
		},
		{
			Name: "enabled",
			Kind: typing.Boolean,
		},
	}

	fqTable := "demo.public.experiments"
	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, nil, true))

	err := s.store.alterTable(fqTable, s.store.configMap.TableConfig(fqTable).CreateTable, config.Add, time.Now().UTC(), cols...)
	assert.NoError(s.T(), err)

	execQuery, _ := s.fakeStore.ExecArgsForCall(0)
	assert.Equal(s.T(), strings.Contains(execQuery, "CREATE TABLE IF NOT EXISTS"), true, execQuery)

	execQuery, _ = s.fakeStore.ExecArgsForCall(1)
	assert.Equal(s.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN enabled boolean", fqTable), execQuery, execQuery)
	assert.Equal(s.T(), s.store.configMap.TableConfig(fqTable).CreateTable, false, s.store.configMap.TableConfig(fqTable))
}
func (s *SnowflakeTestSuite) TestAlterComplexObjects() {
	// Test Structs and Arrays
	cols := []typing.Column{
		{
			Name: "preferences",
			Kind: typing.Struct,
		},
		{
			Name: "array_col",
			Kind: typing.Array,
		},
	}

	fqTable := "shop.public.complex_columns"
	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, nil, false))

	err := s.store.alterTable(fqTable, false, config.Add, time.Now().UTC(), cols...)
	execQuery, _ := s.fakeStore.ExecArgsForCall(0)
	assert.Equal(s.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN preferences variant", fqTable), execQuery)

	execQuery, _ = s.fakeStore.ExecArgsForCall(1)
	assert.Equal(s.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN array_col array", fqTable), execQuery)

	assert.Equal(s.T(), len(cols), s.fakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(s.T(), err)
}

func (s *SnowflakeTestSuite) TestAlterIdempotency() {
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.DateTime,
		},
		{
			Name: "id",
			Kind: typing.Integer,
		},
		{
			Name: "order_name",
			Kind: typing.String,
		},
	}

	fqTable := "shop.public.orders"

	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, nil, false))

	s.fakeStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	err := s.store.alterTable(fqTable, false, config.Add, time.Now().UTC(), cols...)
	assert.Equal(s.T(), len(cols), s.fakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(s.T(), err)

	s.fakeStore.ExecReturns(nil, errors.New("table does not exist"))
	err = s.store.alterTable(fqTable, false, config.Add, time.Now().UTC(), cols...)
	assert.Error(s.T(), err)
}

func (s *SnowflakeTestSuite) TestAlterTableAdd() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.DateTime,
		},
		{
			Name: "id",
			Kind: typing.Integer,
		},
		{
			Name: "order_name",
			Kind: typing.String,
		},
	}

	fqTable := "shop.public.orders"
	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, nil, false))

	err := s.store.alterTable(fqTable, false, config.Add, time.Now().UTC(), cols...)
	assert.Equal(s.T(), len(cols), s.fakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := s.store.configMap.TableConfig(fqTable)
	for col, kind := range tableConfig.Columns() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				assert.Equal(s.T(), kind, expCol.Kind, fmt.Sprintf("wrong col kind, col: %s", col))
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.Columns(), cols))
	}
}

func (s *SnowflakeTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.DateTime,
		},
		{
			Name: "id",
			Kind: typing.Integer,
		},
		{
			Name: "name",
			Kind: typing.String,
		},
	}

	fqTable := "shop.public.users"
	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, nil, false))

	err := s.store.alterTable(fqTable, false, config.Delete, time.Now().UTC(), cols...)
	assert.Equal(s.T(), 0, s.fakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := s.store.configMap.TableConfig(fqTable)
	for col := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete(), cols))
	}

	colToActuallyDelete := cols[0].Name
	// Now let's check the timestamp
	assert.True(s.T(), tableConfig.ColumnsToDelete()[colToActuallyDelete].After(time.Now()))
	// Now let's actually try to dial the time back, and it should actually try to delete.
	tableConfig.AddColumnsToDelete(colToActuallyDelete, time.Now().Add(-1*time.Hour))

	err = s.store.alterTable(fqTable, false, config.Delete, time.Now().UTC(), cols...)
	assert.Nil(s.T(), err)
	assert.Equal(s.T(), 1, s.fakeStore.ExecCallCount(), "tried to delete one column")
	execArg, _ := s.fakeStore.ExecArgsForCall(0)
	assert.Equal(s.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTable, config.Delete, colToActuallyDelete))
}

func (s *SnowflakeTestSuite) TestAlterTableDelete() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.DateTime,
		},
		{
			Name: "id",
			Kind: typing.Integer,
		},
		{
			Name: "name",
			Kind: typing.String,
		},
		{
			Name: "col_to_delete",
			Kind: typing.String,
		},
		{
			Name: "answers",
			Kind: typing.String,
		},
	}

	fqTable := "shop.public.users1"

	s.store.configMap.AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.Kind{}, map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * config.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * config.DeletionConfidencePadding),
	}, false))

	err := s.store.alterTable(fqTable, false, config.Delete, time.Now(), cols...)
	assert.Equal(s.T(), 2, s.fakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := s.store.configMap.TableConfig(fqTable)
	for col := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete(), cols))
	}
}

func (s *SnowflakeTestSuite) TestExecuteMergeNilEdgeCase() {
	// This test was written for https://github.com/artie-labs/transfer/pull/26
	// Say the column first_name already exists in Snowflake as "STRING"
	// I want to delete the value, so I update Postgres and set the cell to be null
	// TableData will think the column is invalid and tableConfig will think column = string
	// Before we call merge, it should reconcile it.
	columns := map[string]typing.Kind{
		"first_name":              typing.Invalid,
		config.DeleteColumnMarker: typing.Boolean,
	}

	rowsData := map[string]map[string]interface{}{
		"pk-1": {
			"first_name": nil,
		},
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	tableData := &optimization.TableData{
		Columns:     columns,
		RowsData:    rowsData,
		TopicConfig: topicConfig,
		PrimaryKey:  "id",
		Rows:        1,
	}

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(), types.NewDwhTableConfig(
		map[string]typing.Kind{
			"first_name":              typing.String,
			config.DeleteColumnMarker: typing.Boolean,
		}, nil, false))

	s.store.Merge(context.Background(), tableData)
	assert.Equal(s.T(), tableData.Columns["first_name"], typing.String)
}

func (s *SnowflakeTestSuite) TestExecuteMerge() {
	columns := map[string]typing.Kind{
		"id":                      typing.Integer,
		"created_at":              typing.DateTime,
		"name":                    typing.String,
		config.DeleteColumnMarker: typing.Boolean,
	}

	rowsData := make(map[string]map[string]interface{})

	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
			"id":         fmt.Sprintf("pk-%d", i),
			"created_at": time.Now().String(),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	topicConfig := kafkalib.TopicConfig{
		Database:  "customer",
		TableName: "orders",
		Schema:    "public",
	}

	tableData := &optimization.TableData{
		Columns:     columns,
		RowsData:    rowsData,
		TopicConfig: topicConfig,
		PrimaryKey:  "id",
		Rows:        1,
	}

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(), types.NewDwhTableConfig(columns, nil, false))
	err := s.store.Merge(context.Background(), tableData)
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

	defer s.store.configMap.RemoveTableFromConfig(topicConfig.ToFqName())
	rowsData := make(map[string]map[string]interface{})
	for i := 0; i < 5; i++ {
		rowsData[fmt.Sprintf("pk-%d", i)] = map[string]interface{}{
			"id":         fmt.Sprintf("pk-%d", i),
			"created_at": time.Now().String(),
			"name":       fmt.Sprintf("Robin-%d", i),
		}
	}

	columns := map[string]typing.Kind{
		"id":                      typing.Integer,
		"created_at":              typing.DateTime,
		"name":                    typing.String,
		config.DeleteColumnMarker: typing.Boolean,
	}

	tableData := &optimization.TableData{
		Columns:     columns,
		RowsData:    rowsData,
		TopicConfig: topicConfig,
		PrimaryKey:  "id",
		Rows:        1,
	}

	sflkColumns := map[string]typing.Kind{
		"id":                      typing.Integer,
		"created_at":              typing.DateTime,
		"name":                    typing.String,
		config.DeleteColumnMarker: typing.Boolean,
	}

	sflkColumns["new"] = typing.String

	s.store.configMap.AddTableToConfig(topicConfig.ToFqName(), types.NewDwhTableConfig(sflkColumns, nil, false))

	err := s.store.Merge(context.Background(), tableData)
	assert.Nil(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 1, "called merge")

	// Check the temp deletion table now.
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName()).ColumnsToDelete()), 1,
		s.store.configMap.TableConfig(topicConfig.ToFqName()).ColumnsToDelete())

	_, isOk := s.store.configMap.TableConfig(topicConfig.ToFqName()).ColumnsToDelete()["new"]
	assert.True(s.T(), isOk)

	// Now try to execute merge where 1 of the rows have the column now
	for _, pkMap := range tableData.RowsData {
		pkMap["new"] = "123"
		tableData.Columns = sflkColumns
		break
	}

	err = s.store.Merge(context.Background(), tableData)
	assert.NoError(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 2, "called merge again")

	// Caught up now, so columns should be 0.
	assert.Equal(s.T(), len(s.store.configMap.TableConfig(topicConfig.ToFqName()).ColumnsToDelete()), 0,
		s.store.configMap.TableConfig(topicConfig.ToFqName()).ColumnsToDelete())
}

func (s *SnowflakeTestSuite) TestExecuteMergeExitEarly() {
	err := s.store.Merge(context.Background(), &optimization.TableData{
		Columns:                 nil,
		RowsData:                nil,
		PrimaryKey:              "",
		TopicConfig:             kafkalib.TopicConfig{},
		PartitionsToLastMessage: nil,
		LatestCDCTs:             time.Time{},
		Rows:                    0,
	})

	assert.Nil(s.T(), err)
}
