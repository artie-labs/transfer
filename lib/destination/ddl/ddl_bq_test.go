package ddl_test

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
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

	tableID := d.bigQueryStore.IdentifierFor(td.TopicConfig().BuildDatabaseAndSchemaPair(), td.Name())
	fqName := tableID.FullyQualifiedName()
	originalColumnLength := len(cols.GetColumns())
	d.bigQueryStore.GetConfigMap().AddTable(tableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	tc := d.bigQueryStore.GetConfigMap().GetTableConfig(tableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Empty(d.T(), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, tc, tableID, []columns.Column{column}, ts, true))
	}

	// Have not deleted, but tried to!
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Len(d.T(), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns(), originalColumnLength)

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, tc, tableID, []columns.Column{column}, ts.Add(2*constants.DeletionConfidencePadding), true))
		_, query, _ := d.fakeBigQueryStore.ExecContextArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", fqName, d.bigQueryStore.Dialect().QuoteIdentifier(column.Name())), query)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Len(d.T(), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns(), 0)
	assert.Equal(d.T(), originalColumnLength, d.fakeBigQueryStore.ExecContextCallCount())
}

func (d *DDLTestSuite) TestAlterTableAddColumns() {
	tableID := dialect.NewTableIdentifier("", "mock_dataset", "add_cols")
	fqName := tableID.FullyQualifiedName()
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

	d.bigQueryStore.GetConfigMap().AddTable(tableID, types.NewDestinationTableConfig(existingCols.GetColumns(), true))
	// Prior to adding, there should be no colsToDelete
	assert.Len(d.T(), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete(), 0)
	assert.Len(d.T(), existingCols.GetColumns(), len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns()))

	var callIdx int
	tc := d.bigQueryStore.GetConfigMap().GetTableConfig(tableID)
	for name, kind := range newCols {
		col := columns.NewColumn(name, kind)
		assert.NoError(d.T(), shared.AlterTableAddColumns(d.T().Context(), d.bigQueryStore, tc, config.SharedDestinationColumnSettings{}, tableID, []columns.Column{col}))

		_, query, _ := d.fakeBigQueryStore.ExecContextArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", fqName, d.bigQueryStore.Dialect().QuoteIdentifier(col.Name()),
			d.bigQueryStore.Dialect().DataTypeForKind(kind, false, config.SharedDestinationColumnSettings{})), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), newColsLen+existingColsLen, len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns()))
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns() {
		existingCol, ok := existingCols.GetColumn(column.Name())
		if !ok {
			// Check new cols?
			existingCol.KindDetails, ok = newCols[column.Name()]
		}

		assert.True(d.T(), ok)
		assert.Equal(d.T(), existingCol.KindDetails, column.KindDetails, existingCol)
	}
}

func (d *DDLTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	tableID := dialect.NewTableIdentifier("", "mock_dataset", "add_cols")
	fqName := tableID.FullyQualifiedName()
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

	d.bigQueryStore.GetConfigMap().AddTable(tableID, types.NewDestinationTableConfig(existingCols.GetColumns(), true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete())
	assert.Len(d.T(), existingCols.GetColumns(), len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns()))

	tc := d.bigQueryStore.GetConfigMap().GetTableConfig(tableID)
	var callIdx int
	for _, column := range existingCols.GetColumns() {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		d.fakeBigQueryStore.ExecContextReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))

		assert.NoError(d.T(), shared.AlterTableAddColumns(d.T().Context(), d.bigQueryStore, tc, config.SharedDestinationColumnSettings{}, tableID, []columns.Column{column}))
		_, query, _ := d.fakeBigQueryStore.ExecContextArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", fqName, d.bigQueryStore.Dialect().QuoteIdentifier(column.Name()),
			d.bigQueryStore.Dialect().DataTypeForKind(column.KindDetails, false, config.SharedDestinationColumnSettings{})), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Len(d.T(), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns(), existingColsLen)
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).GetColumns() {
		existingCol, ok := existingCols.GetColumn(column.Name())
		assert.True(d.T(), ok)
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

	tableID := d.bigQueryStore.IdentifierFor(td.TopicConfig().BuildDatabaseAndSchemaPair(), td.Name())
	d.bigQueryStore.GetConfigMap().AddTable(tableID, types.NewDestinationTableConfig(cols.GetColumns(), false))
	tc := d.bigQueryStore.GetConfigMap().GetTableConfig(tableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().GetTableConfig(tableID).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, tc, tableID, []columns.Column{column}, ts, false))
	}

	// Because containsOtherOperations is false, it should have never tried to delete.
	assert.Equal(d.T(), 0, d.fakeBigQueryStore.ExecContextCallCount())

	// Timestamp got increased, but containsOtherOperations is false, so it should not have tried to delete.
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, tc, tableID, []columns.Column{column}, ts.Add(2*constants.DeletionConfidencePadding), false))
	}

	assert.Equal(d.T(), 0, d.fakeBigQueryStore.ExecContextCallCount())
}
