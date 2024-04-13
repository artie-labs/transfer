package ddl_test

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/ptr"

	artieSQL "github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuery() {
	ts := time.Now()

	td := &optimization.TableData{
		TopicConfig: kafkalib.TopicConfig{
			Database:  "mock_dataset",
			TableName: "delete_col",
		},
	}

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

	fqName := d.bigQueryStore.ToFullyQualifiedName(td, true)
	originalColumnLength := len(cols.GetColumns())
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&cols, nil, false, true))
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     tc,
			FqTableName:            fqName,
			CreateTable:            tc.CreateTable(),
			ColumnOp:               constants.Delete,
			ContainOtherOperations: true,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Have not deleted, but tried to!
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	// Now try to delete again and with an increased TS. It should now be all deleted.
	var callIdx int
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     tc,
			FqTableName:            fqName,
			CreateTable:            tc.CreateTable(),
			ColumnOp:               constants.Delete,
			ContainOtherOperations: true,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", fqName, column.Name(false, &artieSQL.NameArgs{
			Escape:   true,
			DestKind: d.bigQueryStore.Label(),
		})), query)
		callIdx += 1
	}

	// Columns have now been deleted.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	// Columns have not been deleted yet.
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	assert.Equal(d.T(), originalColumnLength, d.fakeBigQueryStore.ExecCallCount())
}

func (d *DDLTestSuite) TestAlterTableAddColumns() {
	fqName := "mock_dataset.add_cols"
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

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&existingCols, nil, false, true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	var callIdx int
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	for name, kind := range newCols {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               d.bigQueryStore,
			Tc:                tc,
			FqTableName:       fqName,
			CreateTable:       tc.CreateTable(),
			ColumnOp:          constants.Add,
			CdcTime:           ts,
			UppercaseEscNames: ptr.ToBool(false),
			Mode:              config.Replication,
		}

		col := columns.NewColumn(name, kind)

		assert.NoError(d.T(), alterTableArgs.AlterTable(col))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, col.Name(false, &artieSQL.NameArgs{
			Escape:   true,
			DestKind: d.bigQueryStore.Label(),
		}), typing.KindToDWHType(kind, d.bigQueryStore.Label(), false)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), newColsLen+existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.RawName())
		if !isOk {
			// Check new cols?
			existingCol.KindDetails, isOk = newCols[column.RawName()]
		}

		assert.True(d.T(), isOk)
		assert.Equal(d.T(), existingCol.KindDetails, column.KindDetails, existingCol)
	}
}

func (d *DDLTestSuite) TestAlterTableAddColumnsSomeAlreadyExist() {
	fqName := "mock_dataset.add_cols"
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

	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&existingCols, nil, false, true))
	// Prior to adding, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), len(existingCols.GetColumns()), len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())

	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	var callIdx int
	for _, column := range existingCols.GetColumns() {
		var sqlResult sql.Result
		// BQ returning the same error because the column already exists.
		d.fakeBigQueryStore.ExecReturnsOnCall(0, sqlResult, errors.New("Column already exists: _string at [1:39]"))
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               d.bigQueryStore,
			Tc:                tc,
			FqTableName:       fqName,
			CreateTable:       tc.CreateTable(),
			ColumnOp:          constants.Add,
			CdcTime:           ts,
			UppercaseEscNames: ptr.ToBool(false),
			Mode:              config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
		query, _ := d.fakeBigQueryStore.ExecArgsForCall(callIdx)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s %s COLUMN %s %s", fqName, constants.Add, column.Name(false, &artieSQL.NameArgs{
			Escape:   true,
			DestKind: d.bigQueryStore.Label(),
		}), typing.KindToDWHType(column.KindDetails, d.bigQueryStore.Label(), false)), query)
		callIdx += 1
	}

	// Check all the columns, make sure it's correct. (length)
	assert.Equal(d.T(), existingColsLen, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns())
	// Check by iterating over the columns
	for _, column := range d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns() {
		existingCol, isOk := existingCols.GetColumn(column.RawName())
		assert.True(d.T(), isOk)
		assert.Equal(d.T(), column.KindDetails, existingCol.KindDetails)
	}
}

func (d *DDLTestSuite) TestAlterTableDropColumnsBigQuerySafety() {
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

	var cols columns.Columns
	for colName, kindDetails := range columnNameToKindDetailsMap {
		cols.AddColumn(columns.NewColumn(colName, kindDetails))
	}

	fqName := d.bigQueryStore.ToFullyQualifiedName(td, true)
	originalColumnLength := len(columnNameToKindDetailsMap)
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&cols, nil, false, false))
	tc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()), d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete())
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               d.bigQueryStore,
			Tc:                tc,
			FqTableName:       fqName,
			CreateTable:       tc.CreateTable(),
			ColumnOp:          constants.Delete,
			CdcTime:           ts,
			UppercaseEscNames: ptr.ToBool(false),
			Mode:              config.Replication,
		}
		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()))

	// Now try to delete again and with an increased TS. It should now be all deleted.
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               d.bigQueryStore,
			Tc:                tc,
			FqTableName:       fqName,
			CreateTable:       tc.CreateTable(),
			ColumnOp:          constants.Delete,
			CdcTime:           ts.Add(2 * constants.DeletionConfidencePadding),
			UppercaseEscNames: ptr.ToBool(false),
			Mode:              config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
		assert.Equal(d.T(), 0, d.fakeBigQueryStore.ExecCallCount())
	}

	// Columns still exist
	assert.Equal(d.T(), 0, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).ReadOnlyColumnsToDelete()))
	assert.Equal(d.T(), originalColumnLength, len(d.bigQueryStore.GetConfigMap().TableConfig(fqName).Columns().GetColumns()))
}
