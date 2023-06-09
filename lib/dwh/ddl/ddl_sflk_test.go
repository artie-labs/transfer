package ddl_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (d *DDLTestSuite) TestAlterComplexObjects() {
	ctx := context.Background()
	// Test Structs and Arrays
	cols := []typing.Column{
		typing.NewColumn("preferences", typing.Struct),
		typing.NewColumn("array_col", typing.Array),
		typing.NewColumn("select", typing.String),
	}

	fqTable := "shop.public.complex_columns"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Add,
		CdcTime:     time.Now().UTC(),
	}

	err := ddl.AlterTable(ctx, alterTableArgs, cols...)

	for i := 0; i < len(cols); i++ {
		execQuery, _ := d.fakeSnowflakeStore.ExecArgsForCall(i)
		assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN %s %s", fqTable, cols[i].Name(true),
			typing.KindToDWHType(cols[i].KindDetails, d.snowflakeStore.Label())), execQuery)
	}

	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)
}

func (d *DDLTestSuite) TestAlterIdempotency() {
	ctx := context.Background()
	cols := []typing.Column{
		typing.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		typing.NewColumn("id", typing.Integer),
		typing.NewColumn("order_name", typing.String),
		typing.NewColumn("start", typing.String),
	}

	fqTable := "shop.public.orders"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	d.fakeSnowflakeStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Add,
		CdcTime:     time.Now().UTC(),
	}

	err := ddl.AlterTable(ctx, alterTableArgs, cols...)
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)

	d.fakeSnowflakeStore.ExecReturns(nil, errors.New("table does not exist"))
	err = ddl.AlterTable(ctx, alterTableArgs, cols...)
	assert.Error(d.T(), err)
}

func (d *DDLTestSuite) TestAlterTableAdd() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		typing.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		typing.NewColumn("id", typing.Integer),
		typing.NewColumn("order_name", typing.String),
		typing.NewColumn("start", typing.String),
	}

	fqTable := "shop.public.orders"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Add,
		CdcTime:     time.Now().UTC(),
	}
	err := ddl.AlterTable(d.ctx, alterTableArgs, cols...)
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)

	// Check the table config
	tableConfig := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	for _, column := range tableConfig.Columns().GetColumns() {
		var found bool
		for _, expCol := range cols {
			if found = column.Name(false) == expCol.Name(false); found {
				assert.Equal(d.T(), column.KindDetails, expCol.KindDetails, fmt.Sprintf("wrong col kind, col: %s", column.Name(false)))
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				column.Name(false), tableConfig.Columns(), cols))
	}
}

func (d *DDLTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		typing.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		typing.NewColumn("id", typing.Integer),
		typing.NewColumn("name", typing.String),
		typing.NewColumn("start", typing.String),
	}

	fqTable := "shop.public.users"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Delete,
		CdcTime:     time.Now().UTC(),
	}
	err := ddl.AlterTable(d.ctx, alterTableArgs, cols...)
	assert.Equal(d.T(), 0, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(d.T(), err)

	// Check the table config
	tableConfig := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	for col := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name(false); found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ReadOnlyColumnsToDelete(), cols))
	}

	for i := 0; i < len(cols); i++ {
		colToActuallyDelete := cols[i].Name(false)
		// Now let's check the timestamp
		assert.True(d.T(), tableConfig.ReadOnlyColumnsToDelete()[colToActuallyDelete].After(time.Now()))
		// Now let's actually try to dial the time back, and it should actually try to delete.
		tableConfig.AddColumnsToDelete(colToActuallyDelete, time.Now().Add(-1*time.Hour))

		err = ddl.AlterTable(d.ctx, alterTableArgs, cols...)
		assert.Nil(d.T(), err)
		assert.Equal(d.T(), i+1, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete one column")

		execArg, _ := d.fakeSnowflakeStore.ExecArgsForCall(i)
		assert.Equal(d.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTable, constants.Delete, cols[i].Name(true)))
	}
}

func (d *DDLTestSuite) TestAlterTableDelete() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		typing.NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)),
		typing.NewColumn("id", typing.Integer),
		typing.NewColumn("name", typing.String),
		typing.NewColumn("col_to_delete", typing.String),
		typing.NewColumn("answers", typing.String),
		typing.NewColumn("start", typing.String),
	}

	fqTable := "shop.public.users1"

	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"start":         time.Now().Add(-2 * constants.DeletionConfidencePadding),
	}, false, true))

	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Delete,
		CdcTime:     time.Now(),
	}
	err := ddl.AlterTable(d.ctx, alterTableArgs, cols...)
	assert.Equal(d.T(), 3, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(d.T(), err)

	// Check the table config
	tableConfig := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	for col := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name(false); found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ReadOnlyColumnsToDelete(), cols))
	}
}
