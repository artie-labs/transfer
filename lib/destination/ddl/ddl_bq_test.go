package ddl_test

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuery() {
	ts := time.Now()

	td := optimization.NewTableData(nil, config.Replication, nil,
		kafkalib.TopicConfig{
			Database:  "mock_dataset",
			TableName: "delete_col"},
		"delete_col")

	colNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo":    typing.String,
		"bar":    typing.String,
		"select": typing.String,
		"start":  typing.String,
	}

	var cols columns.Columns
	for colName, kindDetails := range colNameToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	tableID := d.bigQueryStore.IdentifierFor(td.TopicConfig(), td.Name())
	fqName := tableID.FullyQualifiedName()
	originalColumnLength := len(cols.GetColumns())
	d.bigQueryStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	tc := d.bigQueryStore.GetConfigMap().TableConfigCache(tableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     tc,
			TableID:                tableID,
			CreateTable:            tc.CreateTable(),
			ColumnOp:               constants.Delete,
			ContainOtherOperations: true,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
	}

	// Have not deleted, but tried to!
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     tc,
			TableID:                tableID,
			CreateTable:            tc.CreateTable(),
			ColumnOp:               constants.Delete,
			ContainOtherOperations: true,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", fqName, d.bigQueryStore.Dialect().QuoteIdentifier(column.Name())), query)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())
	assert.Equal(d.T(), originalColumnLength, d.fakeBigQueryStore.ExecCallCount())
}

func (d *DDLTestSuite) TestAlterTableAddColumns() {
	tableID := bigquery.NewTableIdentifier("", "mock_dataset", "add_cols")
	fqName := tableID.FullyQualifiedName()
	ts := time.Now()
	existingColNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	newCols := map[string]typing.KindDetails{
		"dusty":      typing.String,
		"jacqueline": typing.Integer,
		"charlie":    typing.Boolean,
		"robin":      typing.Float,
		"start":      typing.String,
	}

	newColsLen := len(newCols)
	existingColsLen := len(existingColNameToKindDetailsMap)
	var existingCols columns.Columns
	for colName, kindDetails := range existingColNameToKindDetailsMap {
		existingCols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	d.bigQueryStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(existingCols.GetColumns(), true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())

	var callIdx int
	tc := d.bigQueryStore.GetConfigMap().TableConfigCache(tableID)
	for name, kind := range newCols {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     d.bigQueryStore.Dialect(),
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Add,
			CdcTime:     ts,
			Mode:        config.Replication,
		}

		col := columns.NewColumn(name, kind)

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, col))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, d.bigQueryStore.Dialect().QuoteIdentifier(col.Name()),
			d.bigQueryStore.Dialect().DataTypeForKind(kind, false)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), newColsLen+existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.Name())
		if !isOk {
			// Check new cols?
			existingCol.KindDetails, isOk = newCols[column.Name()]
		}

		assert.True(d.T(), isOk)
		assert.Equal(d.T(), existingCol.KindDetails, column.KindDetails, existingCol)
	}
}

func (d *DDLTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	tableID := bigquery.NewTableIdentifier("", "mock_dataset", "add_cols")
	fqName := tableID.FullyQualifiedName()
	ts := time.Now()
	existingColNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo":   typing.String,
		"bar":   typing.String,
		"start": typing.String,
	}

	existingColsLen := len(existingColNameToKindDetailsMap)
	var existingCols columns.Columns
	for colName, kindDetails := range existingColNameToKindDetailsMap {
		existingCols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	d.bigQueryStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(existingCols.GetColumns(), true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())

	tc := d.bigQueryStore.GetConfigMap().TableConfigCache(tableID)
	var callIdx int
	for _, column := range existingCols.GetColumns() {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		d.fakeBigQueryStore.ExecReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     d.bigQueryStore.Dialect(),
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Add,
			CdcTime:     ts,
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, d.bigQueryStore.Dialect().QuoteIdentifier(column.Name()),
			d.bigQueryStore.Dialect().DataTypeForKind(column.KindDetails, false)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.Name())
		assert.True(d.T(), isOk)
		assert.Equal(d.T(), column.KindDetails, existingCol.KindDetails)
	}
}

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuerySafety() {
	ts := time.Now()
	td := optimization.NewTableData(nil, config.Replication, nil,
		kafkalib.TopicConfig{
			Database:  "mock_dataset",
			TableName: "delete_col",
		}, "foo")

	columnNameToKindDetailsMap := map[string]typing.KindDetails{
		"foo": typing.String,
		"bar": typing.String,
	}

	var cols columns.Columns
	for colName, kindDetails := range columnNameToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	tableID := d.bigQueryStore.IdentifierFor(td.TopicConfig(), td.Name())
	originalColumnLength := len(columnNameToKindDetailsMap)
	d.bigQueryStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(cols.GetColumns(), false))
	tc := d.bigQueryStore.GetConfigMap().TableConfigCache(tableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     d.bigQueryStore.Dialect(),
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Delete,
			CdcTime:     ts,
			Mode:        config.Replication,
		}
		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
	}

	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()))

	// Now try to delete again and with an increased TS. It should now be all deleted.
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     d.bigQueryStore.Dialect(),
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Delete,
			CdcTime:     ts.Add(2 * constants.DeletionConfidencePadding),
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
		assert.Equal(d.T(), 0, d.fakeBigQueryStore.ExecCallCount())
	}

	// Columns still exist
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).ReadOnlyColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfigCache(tableID).Columns().GetColumns()))
}
