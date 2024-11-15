package ddl_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/artie-labs/transfer/clients/shared"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestAlterComplexObjects() {
	// Test Structs and Arrays
	cols := []columns.Column{
		columns.NewColumn("preferences", typing.Struct),
		columns.NewColumn("array_col", typing.Array),
		columns.NewColumn("select", typing.String),
	}

	tableID := dialect.NewTableIdentifier("shop", "public", "complex_columns")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)

	alterTableArgs := ddl.AlterTableArgs{
		Dialect:  d.snowflakeStagesStore.Dialect(),
		Tc:       tc,
		TableID:  tableID,
		ColumnOp: constants.Add,
		CdcTime:  time.Now().UTC(),
		Mode:     config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...))
	for i := 0; i < len(cols); i++ {
		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(i)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN %s %s", `shop.public."COMPLEX_COLUMNS"`,
			d.snowflakeStagesStore.Dialect().QuoteIdentifier(cols[i].Name()),
			d.snowflakeStagesStore.Dialect().DataTypeForKind(cols[i].KindDetails, false)), execQuery)
	}

	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")
}

func (d *DDLTestSuite) TestAlterIdempotency() {
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.TimestampTZ),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("order_name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := dialect.NewTableIdentifier("shop", "public", "orders")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)

	d.fakeSnowflakeStagesStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	alterTableArgs := ddl.AlterTableArgs{
		Dialect:  d.snowflakeStagesStore.Dialect(),
		Tc:       tc,
		TableID:  tableID,
		ColumnOp: constants.Add,
		CdcTime:  time.Now().UTC(),
		Mode:     config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...))
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")

	d.fakeSnowflakeStagesStore.ExecReturns(nil, errors.New("table does not exist"))
	assert.ErrorContains(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...), "failed to apply ddl")
}

func (d *DDLTestSuite) TestAlterTableAdd() {
	// Test adding a bunch of columns
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.TimestampTZ),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("order_name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := dialect.NewTableIdentifier("shop", "public", "orders")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)

	alterTableArgs := ddl.AlterTableArgs{
		Dialect:  d.snowflakeStagesStore.Dialect(),
		Tc:       tc,
		TableID:  tableID,
		ColumnOp: constants.Add,
		CdcTime:  time.Now().UTC(),
		Mode:     config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...))
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")

	// Check the table config
	tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	for _, column := range tableConfig.GetColumns() {
		var found bool
		for _, expCol := range cols {
			if found = column.Name() == expCol.Name(); found {
				assert.Equal(d.T(), column.KindDetails, expCol.KindDetails, fmt.Sprintf("wrong col kind, col: %s", column.Name()))
				break
			}
		}

		assert.True(d.T(), found, fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v", column.Name(), tableConfig.GetColumns(), cols))
	}
}

func (d *DDLTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.TimestampTZ),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := dialect.NewTableIdentifier("shop", "public", "users")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	alterTableArgs := ddl.AlterTableArgs{
		Dialect:                d.snowflakeStagesStore.Dialect(),
		Tc:                     tc,
		TableID:                tableID,
		ContainOtherOperations: true,
		ColumnOp:               constants.Delete,
		CdcTime:                time.Now().UTC(),
		Mode:                   config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...))
	assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecCallCount(), "tried to delete, but not yet.")

	// Check the table config
	tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	for col := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name(); found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ReadOnlyColumnsToDelete(), cols))
	}

	for i := 0; i < len(cols); i++ {
		colToActuallyDelete := cols[i].Name()
		// Now let's check the timestamp
		assert.True(d.T(), tableConfig.ReadOnlyColumnsToDelete()[colToActuallyDelete].After(time.Now()))
		// Now let's actually try to dial the time back, and it should actually try to delete.
		tableConfig.AddColumnsToDelete(colToActuallyDelete, time.Now().Add(-1*time.Hour))

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, cols...))
		assert.Equal(d.T(), i+1, d.fakeSnowflakeStagesStore.ExecCallCount(), "tried to delete one column")

		execArg, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(i)
		assert.Equal(d.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", `shop.public."USERS"`, constants.Delete,
			d.snowflakeStagesStore.Dialect().QuoteIdentifier(cols[i].Name()),
		))
	}
}

func (d *DDLTestSuite) TestAlterTableDelete() {
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.TimestampTZ),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn("col_to_delete", typing.String),
		columns.NewColumn("answers", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := dialect.NewTableIdentifier("shop", "public", "users1")
	tableCfg := types.NewDwhTableConfig(nil, true)
	colsToDeleteMap := map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"start":         time.Now().Add(-2 * constants.DeletionConfidencePadding),
	}
	tableCfg.SetColumnsToDelete(colsToDeleteMap)
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, tableCfg)
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	{
		// containsOtherOperations = false
		assert.NoError(d.T(), shared.AlterTableDropColumns(context.Background(), d.snowflakeStagesStore, tc, tableID, cols, time.Now(), false))
		// Nothing got deleted
		assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecContextCallCount())
		assert.Equal(d.T(), colsToDeleteMap, tc.ReadOnlyColumnsToDelete())
	}
	{
		// containsOtherOperations = true
		assert.NoError(d.T(), shared.AlterTableDropColumns(context.Background(), d.snowflakeStagesStore, tc, tableID, cols, time.Now(), true))
		assert.Equal(d.T(), 3, d.fakeSnowflakeStagesStore.ExecContextCallCount())

		// Check the table config
		tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)

		var colsToDelete []string
		for col := range tableConfig.ReadOnlyColumnsToDelete() {
			colsToDelete = append(colsToDelete, col)
		}

		// Cols that should have been deleted, have been. The rest are still there the reserve.
		assert.Len(d.T(), colsToDelete, 3)
		slices.Sort(colsToDelete)
		assert.Equal(d.T(), []string{"created_at", "id", "name"}, colsToDelete)
	}
}
