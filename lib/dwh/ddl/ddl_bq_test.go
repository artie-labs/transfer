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

	columns := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	fqName := td.ToFqName(constants.BigQuery)

	originalColumnLength := len(columns)
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(columns, nil, false))

	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	tc.DropDeletedColumns = true

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	for name, kind := range columns {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts, typing.Column{Name: name, Kind: kind})
		assert.NoError(d.T(), err)
	}

	// Have not deleted, but tried to!
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for name, kind := range columns {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Delete, ts.Add(2*constants.DeletionConfidencePadding),
			typing.Column{
				Name: name,
				Kind: kind,
			})

		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", fqName, name), query)
		assert.NoError(d.T(), err)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	assert.Equal(d.T(), originalColumnLength, d.fakeBigQueryStore.ExecCallCount())
}

func (d *DDLTestSuite) TestAlterTableAddColumns() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingCols := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}
	existingColsLen := len(existingCols)

	newCols := map[string]typing.KindDetails{
		"dusty":      typing.String,
		"jacqueline": typing.Integer,
		"charlie":    typing.Boolean,
		"robin":      typing.Float,
	}
	newColsLen := len(newCols)

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	assert.Equal(d.T(), len(existingCols), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	var callIdx int
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	for name, kind := range newCols {
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, ts, typing.Column{Name: name, Kind: kind})
		assert.NoError(d.T(), err)
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, name, typing.KindToDWHType(kind, d.bigQueryStore.Label())), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), newColsLen+existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for tableCol, tableColKind := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns() {
		var isOk bool
		var kind typing.KindDetails
		kind, isOk = existingCols[tableCol]
		if !isOk {
			kind, isOk = newCols[tableCol]
		}

		assert.Equal(d.T(), tableColKind, kind)
		assert.True(d.T(), isOk)
	}
}

func (d *DDLTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	fqName := "mock_dataset.add_cols"
	ctx := context.Background()
	ts := time.Now()
	existingCols := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	existingColsLen := len(existingCols)

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(existingCols, nil, false))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ColumnsToDelete())
	assert.Equal(d.T(), len(existingCols), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	var callIdx int
	for name, kind := range existingCols {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		d.fakeBigQueryStore.ExecReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))
		err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, ts, typing.Column{Name: name, Kind: kind})
		assert.NoError(d.T(), err)
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, name, typing.KindToDWHType(kind, d.bigQueryStore.Label())), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for tableCol, tableColKind := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns() {
		kind, isOk := existingCols[tableCol]
		assert.Equal(d.T(), tableColKind, kind)
		assert.True(d.T(), isOk)
	}
}

func (d *DDLTestSuite) TestAlterTableCreateTable() {
	fqName := "mock_dataset.mock_table"
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(nil, nil, true))

	ctx := context.Background()
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	err := ddl.AlterTable(ctx, d.bigQueryStore, tc, fqName, tc.CreateTable, constants.Add, time.Time{}, typing.Column{Name: "name", Kind: typing.String})
	assert.Equal(d.T(), 1, d.fakeBigQueryStore.ExecCallCount())

	query, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
	assert.Equal(d.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", fqName), query)
	assert.NoError(d.T(), err, err)
	assert.Equal(d.T(), false, d.bigQueryStore.GetConfigMap().TableConfig(fqName).CreateTable)
}
