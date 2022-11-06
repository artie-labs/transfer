package snowflake

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

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
	mdConfig.snowflakeTableToConfig[fqTable] = &snowflakeTableConfig{
		Columns: map[string]typing.Kind{},
	}

	s.fakeStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	err := alterTable(fqTable, Add, time.Now().UTC(), cols...)
	assert.Equal(s.T(), len(cols), s.fakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(s.T(), err)

	s.fakeStore.ExecReturns(nil, errors.New("table does not exist"))
	err = alterTable(fqTable, Add, time.Now().UTC(), cols...)
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
	mdConfig.snowflakeTableToConfig[fqTable] = &snowflakeTableConfig{
		Columns: map[string]typing.Kind{},
	}

	err := alterTable(fqTable, Add, time.Now().UTC(), cols...)
	assert.Equal(s.T(), len(cols), s.fakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := mdConfig.snowflakeTableToConfig[fqTable]
	for col, kind := range tableConfig.Columns {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				assert.Equal(s.T(), kind, expCol.Kind, fmt.Sprintf("wrong col kind, col: %s", col))
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.Columns, cols))
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
	mdConfig.snowflakeTableToConfig[fqTable] = &snowflakeTableConfig{
		Columns:         map[string]typing.Kind{},
		ColumnsToDelete: map[string]time.Time{},
	}

	err := alterTable(fqTable, Delete, time.Now().UTC(), cols...)
	assert.Equal(s.T(), 0, s.fakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := mdConfig.snowflakeTableToConfig[fqTable]
	for col := range tableConfig.ColumnsToDelete {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete, cols))
	}
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
	mdConfig.snowflakeTableToConfig[fqTable] = &snowflakeTableConfig{
		Columns: map[string]typing.Kind{},
		ColumnsToDelete: map[string]time.Time{
			"col_to_delete": time.Now().Add(-2 * config.DeletionConfidencePadding),
			"answers":       time.Now().Add(-2 * config.DeletionConfidencePadding),
		},
	}

	err := alterTable(fqTable, Delete, time.Now(), cols...)
	assert.Equal(s.T(), 2, s.fakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(s.T(), err)

	// Check the table config
	tableConfig := mdConfig.snowflakeTableToConfig[fqTable]
	for col := range tableConfig.ColumnsToDelete {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(s.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete, cols))
	}
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
	}

	mdConfig.snowflakeTableToConfig[topicConfig.ToFqName()] = &snowflakeTableConfig{
		Columns: columns,
	}

	err := ExecuteMerge(context.Background(), tableData)
	assert.Nil(s.T(), err)
	s.fakeStore.ExecReturns(nil, nil)
	assert.Equal(s.T(), s.fakeStore.ExecCallCount(), 1, "called merge")
}
