package ddl_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuery() {
	ctx := context.Background()
	ts := time.Now()

	td := &optimization.TableData{
		TopicConfig: kafkalib.TopicConfig{
			Database:  "mock_dataset",
			TableName: "delete_col",
		},
	}

	colNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	var cols typing.Columns
	for colName, kindDetails := range colNameToKindDetailsMap {
		cols.AddColumn(typing.Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	fqName := td.ToFqName(constants.BigQuery)

	originalColumnLength := len(cols.GetColumns())
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(cols, nil, false, true))
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	for _, column := range cols.GetColumns() {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts, column)
		assert.NoError(d.T(), err)
	}

	// Have not deleted, but tried to!
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for _, column := range cols.GetColumns() {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts.Add(2*constants.DeletionConfidencePadding), column)

		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", fqName, column.Name), query)
		assert.NoError(d.T(), err)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	assert.Equal(d.T(), originalColumnLength, d.fakeBigQueryStore.ExecCallCount())
}

func (d *DDLTestSuite) TestAlterTableAddColumns() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingColNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}
	existingColsLen := len(existingColNameToKindDetailsMap)

	newCols := map[string]typing.KindDetails{
		"dusty":      typing.String,
		"jacqueline": typing.Integer,
		"charlie":    typing.Boolean,
		"robin":      typing.Float,
	}
	newColsLen := len(newCols)

	var existingCols typing.Columns
	for colName, kindDetails := range existingColNameToKindDetailsMap {
		existingCols.AddColumn(typing.Column{Name: colName, KindDetails: kindDetails})
	}

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false, true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	var callIdx int
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	for name, kind := range newCols {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, ts, typing.Column{Name: name, KindDetails: kind})
		assert.NoError(d.T(), err)
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, name, typing.KindToDWHType(kind, d.bigQueryStore.Label())), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), newColsLen+existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.Name)
		assert.True(d.T(), isOk)
		assert.Equal(d.T(), existingCol.KindDetails, column.KindDetails)
	}
}

func (d *DDLTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingColNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	existingColsLen := len(existingColNameToKindDetailsMap)
	var existingCols typing.Columns
	for colName, kindDetails := range existingColNameToKindDetailsMap {
		existingCols.AddColumn(typing.Column{Name: colName, KindDetails: kindDetails})
	}

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false, true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	var callIdx int
	for _, column := range existingCols.GetColumns() {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		d.fakeBigQueryStore.ExecReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, ts, column)
		assert.NoError(d.T(), err)
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, column.Name, typing.KindToDWHType(column.KindDetails, d.bigQueryStore.Label())), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.Name)
		assert.True(d.T(), isOk)
		assert.Equal(d.T(), column.KindDetails, existingCol.KindDetails)
	}
}

func (d *DDLTestSuite) TestAlterTableCreateTable() {
	fqName := "mock_dataset.mock_table"
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(typing.Columns{}, nil, true, true))

	ctx := context.Background()
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, time.Time{}, typing.Column{Name: "name", KindDetails: typing.String})
	assert.Equal(d.T(), 1, d.fakeBigQueryStore.ExecCallCount())

	query, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
	assert.Equal(d.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", fqName), query)
	assert.NoError(d.T(), err, err)
	assert.Equal(d.T(), false, d.bigQueryStore.GetConfigMap().TableConfig(fqName).CreateTable)
}

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuerySafety() {
	ctx := context.Background()
	ts := time.Now()

	td := &optimization.TableData{
		TopicConfig: kafkalib.TopicConfig{
			Database:  "mock_dataset",
			TableName: "delete_col",
		},
	}

	columnNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	var columns typing.Columns
	for colName, kindDetails := range columnNameToKindDetailsMap {
		columns.AddColumn(typing.Column{Name: colName, KindDetails: kindDetails})
	}

	fqName := td.ToFqName(constants.BigQuery)

	originalColumnLength := len(columnNameToKindDetailsMap)
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(columns, nil, false, false))
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	for _, column := range columns.GetColumns() {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts, column)
		assert.NoError(d.T(), err)
	}

	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()))

	// Now try to delete again and with an increased TS. It should now be all deleted.
	for _, column := range columns.GetColumns() {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts.Add(2*constants.DeletionConfidencePadding), column)
		assert.NoError(d.T(), err)
		assert.Equal(d.T(), 0, d.fakeBigQueryStore.ExecCallCount())
	}

	// Columns still exist
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()))
}
