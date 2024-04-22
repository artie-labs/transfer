package ddl_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (d *DDLTestSuite) TestAlterComplexObjects() {
	// Test Structs and Arrays
	cols := []columns.Column{
		columns.NewColumn("preferences", typing.Struct),
		columns.NewColumn("array_col", typing.Array),
		columns.NewColumn("select", typing.String),
	}

	tableID := snowflake.NewTableIdentifier("shop", "public", "complex_columns", true)
	fqTable := "shop.public.complex_columns"
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, nil, false, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:               d.snowflakeStagesStore,
		Tc:                tc,
		TableID:           tableID,
		ColumnOp:          constants.Add,
		CdcTime:           time.Now().UTC(),
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
	for i := 0; i < len(cols); i++ {
		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(i)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN %s %s", fqTable, cols[i].Name(false, &columns.NameArgs{
			DestKind: d.snowflakeStagesStore.Label(),
		}),
			typing.KindToDWHType(cols[i].KindDetails, d.snowflakeStagesStore.Label(), false)), execQuery)
	}

	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")
}

func (d *DDLTestSuite) TestAlterIdempotency() {
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("order_name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := snowflake.NewTableIdentifier("shop", "public", "orders", true)
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, nil, false, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)

	d.fakeSnowflakeStagesStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:               d.snowflakeStagesStore,
		Tc:                tc,
		TableID:           tableID,
		ColumnOp:          constants.Add,
		CdcTime:           time.Now().UTC(),
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")

	d.fakeSnowflakeStagesStore.ExecReturns(nil, errors.New("table does not exist"))
	assert.ErrorContains(d.T(), alterTableArgs.AlterTable(cols...), "failed to apply ddl")
}

func (d *DDLTestSuite) TestAlterTableAdd() {
	// Test adding a bunch of columns
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("order_name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := snowflake.NewTableIdentifier("shop", "public", "orders", true)
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, nil, false, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:               d.snowflakeStagesStore,
		Tc:                tc,
		TableID:           tableID,
		ColumnOp:          constants.Add,
		CdcTime:           time.Now().UTC(),
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStagesStore.ExecCallCount(), "called SFLK the same amt to create cols")

	// Check the table config
	tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)
	for _, column := range tableConfig.Columns().GetColumns() {
		var found bool
		for _, expCol := range cols {
			if found = column.RawName() == expCol.RawName(); found {
				assert.Equal(d.T(), column.KindDetails, expCol.KindDetails, fmt.Sprintf("wrong col kind, col: %s", column.RawName()))
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				column.RawName(), tableConfig.Columns(), cols))
	}
}

func (d *DDLTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := snowflake.NewTableIdentifier("shop", "public", "users", true)
	fqTable := "shop.public.users"
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, nil, false, true))
	tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:                    d.snowflakeStagesStore,
		Tc:                     tc,
		TableID:                tableID,
		ContainOtherOperations: true,
		ColumnOp:               constants.Delete,
		CdcTime:                time.Now().UTC(),
		UppercaseEscNames:      ptr.ToBool(false),
		Mode:                   config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
	assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecCallCount(), "tried to delete, but not yet.")

	// Check the table config
	tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)
	for col := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.RawName(); found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ReadOnlyColumnsToDelete(), cols))
	}

	for i := 0; i < len(cols); i++ {
		colToActuallyDelete := cols[i].RawName()
		// Now let's check the timestamp
		assert.True(d.T(), tableConfig.ReadOnlyColumnsToDelete()[colToActuallyDelete].After(time.Now()))
		// Now let's actually try to dial the time back, and it should actually try to delete.
		tableConfig.AddColumnsToDelete(colToActuallyDelete, time.Now().Add(-1*time.Hour))

		assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
		assert.Equal(d.T(), i+1, d.fakeSnowflakeStagesStore.ExecCallCount(), "tried to delete one column")

		execArg, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(i)
		assert.Equal(d.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTable, constants.Delete,
			cols[i].Name(false, &columns.NameArgs{DestKind: d.snowflakeStagesStore.Label()})))
	}
}

func (d *DDLTestSuite) TestAlterTableDelete() {
	// Test adding a bunch of columns
	cols := []columns.Column{
		columns.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn("col_to_delete", typing.String),
		columns.NewColumn("answers", typing.String),
		columns.NewColumn("start", typing.String),
	}

	tableID := snowflake.NewTableIdentifier("shop", "public", "users1", true)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"start":         time.Now().Add(-2 * constants.DeletionConfidencePadding),
	}, false, true))

	tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:                    d.snowflakeStagesStore,
		Tc:                     tc,
		TableID:                tableID,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: true,
		CdcTime:                time.Now(),
		UppercaseEscNames:      ptr.ToBool(false),
		Mode:                   config.Replication,
	}

	assert.NoError(d.T(), alterTableArgs.AlterTable(cols...))
	assert.Equal(d.T(), 3, d.fakeSnowflakeStagesStore.ExecCallCount(), "tried to delete, but not yet.")

	// Check the table config
	tableConfig := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)
	for col := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.RawName(); found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ReadOnlyColumnsToDelete(), cols))
	}
}
