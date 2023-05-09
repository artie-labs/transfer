package ddl_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func (d *DDLTestSuite) TestCreateTable() {
	ctx := context.Background()
	fqTable := "demo.public.experiments"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, nil, true, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	cols := []typing.Column{
		{
			Name:        "key",
			KindDetails: typing.String,
		},
		{
			Name:        "enabled",
			KindDetails: typing.Boolean,
		},
	}

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		CreateTable: tc.CreateTable,
		ColumnOp:    constants.Add,
		CdcTime:     time.Now().UTC(),
	}
	err := ddl.AlterTable(ctx, alterTableArgs, cols...)
	assert.NoError(d.T(), err)

	execQuery, _ := d.fakeSnowflakeStore.ExecArgsForCall(0)
	assert.Equal(d.T(), strings.Contains(execQuery, "CREATE TABLE IF NOT EXISTS"), true, execQuery)

	execQuery, _ = d.fakeSnowflakeStore.ExecArgsForCall(1)
	assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN enabled boolean", fqTable), execQuery, execQuery)
	assert.Equal(d.T(), d.snowflakeStore.GetConfigMap().TableConfig(fqTable).CreateTable, false, d.snowflakeStore.GetConfigMap().TableConfig(fqTable))
}
func (d *DDLTestSuite) TestAlterComplexObjects() {
	ctx := context.Background()
	// Test Structs and Arrays
	cols := []typing.Column{
		{
			Name:        "preferences",
			KindDetails: typing.Struct,
		},
		{
			Name:        "array_col",
			KindDetails: typing.Array,
		},
	}

	fqTable := "shop.public.complex_columns"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	alterTableArgs := ddl.AlterTableArgs{
		Dwh:         d.snowflakeStore,
		Tc:          tc,
		FqTableName: fqTable,
		ColumnOp:    constants.Add,
		CdcTime:     time.Now().UTC(),
	}
	err := ddl.AlterTable(ctx, alterTableArgs, cols...)
	execQuery, _ := d.fakeSnowflakeStore.ExecArgsForCall(0)
	assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN preferences variant", fqTable), execQuery)

	execQuery, _ = d.fakeSnowflakeStore.ExecArgsForCall(1)
	assert.Equal(d.T(), fmt.Sprintf("ALTER TABLE %s add COLUMN array_col array", fqTable), execQuery)

	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)
}

func (d *DDLTestSuite) TestAlterIdempotency() {
	ctx := context.Background()
	cols := []typing.Column{
		{
			Name:        "created_at",
			KindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			Name:        "id",
			KindDetails: typing.Integer,
		},
		{
			Name:        "order_name",
			KindDetails: typing.String,
		},
	}

	fqTable := "shop.public.orders"

	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, nil, false, true))
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
		{
			Name:        "created_at",
			KindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			Name:        "id",
			KindDetails: typing.Integer,
		},
		{
			Name:        "order_name",
			KindDetails: typing.String,
		},
	}

	fqTable := "shop.public.orders"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, nil, false, true))
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
			if found = column.Name == expCol.Name; found {
				assert.Equal(d.T(), column.KindDetails, expCol.KindDetails, fmt.Sprintf("wrong col kind, col: %s", column.Name))
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				column.Name, tableConfig.Columns(), cols))
	}
}

func (d *DDLTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name:        "created_at",
			KindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			Name:        "id",
			KindDetails: typing.Integer,
		},
		{
			Name:        "name",
			KindDetails: typing.String,
		},
	}

	fqTable := "shop.public.users"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, nil, false, true))
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
	for col := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete(), cols))
	}

	colToActuallyDelete := cols[0].Name
	// Now let's check the timestamp
	assert.True(d.T(), tableConfig.ColumnsToDelete()[colToActuallyDelete].After(time.Now()))
	// Now let's actually try to dial the time back, and it should actually try to delete.
	tableConfig.AddColumnsToDelete(colToActuallyDelete, time.Now().Add(-1*time.Hour))

	err = ddl.AlterTable(d.ctx, alterTableArgs, cols...)
	assert.Nil(d.T(), err)
	assert.Equal(d.T(), 1, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete one column")
	execArg, _ := d.fakeSnowflakeStore.ExecArgsForCall(0)
	assert.Equal(d.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTable, constants.Delete, colToActuallyDelete))
}

func (d *DDLTestSuite) TestAlterTableDelete() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name:        "created_at",
			KindDetails: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		},
		{
			Name:        "id",
			KindDetails: typing.Integer,
		},
		{
			Name:        "name",
			KindDetails: typing.String,
		},
		{
			Name:        "col_to_delete",
			KindDetails: typing.String,
		},
		{
			Name:        "answers",
			KindDetails: typing.String,
		},
	}

	fqTable := "shop.public.users1"

	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(typing.Columns{}, map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * constants.DeletionConfidencePadding),
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
	assert.Equal(d.T(), 2, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete, but not yet.")
	assert.NoError(d.T(), err)

	// Check the table config
	tableConfig := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	for col := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.ColumnsToDelete(), cols))
	}
}
