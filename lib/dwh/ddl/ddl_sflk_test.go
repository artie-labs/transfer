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
	cols := []typing.Column{
		{
			Name: "key",
			Kind: typing.String,
		},
		{
			Name: "enabled",
			Kind: typing.Boolean,
		},
	}

	fqTable := "demo.public.experiments"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, nil, true, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	err := ddl.AlterTable(ctx, d.snowflakeStore, tc, fqTable, tc.CreateTable, constants.Add, time.Now().UTC(), cols...)
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
			Name: "preferences",
			Kind: typing.Struct,
		},
		{
			Name: "array_col",
			Kind: typing.Array,
		},
	}

	fqTable := "shop.public.complex_columns"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	err := ddl.AlterTable(ctx, d.snowflakeStore, tc, fqTable, false, constants.Add, time.Now().UTC(), cols...)
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
			Name: "created_at",
			Kind: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
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

	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	d.fakeSnowflakeStore.ExecReturns(nil, errors.New("column 'order_name' already exists"))
	err := ddl.AlterTable(ctx, d.snowflakeStore, tc, fqTable, false, constants.Add, time.Now().UTC(), cols...)
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)

	d.fakeSnowflakeStore.ExecReturns(nil, errors.New("table does not exist"))
	err = ddl.AlterTable(ctx, d.snowflakeStore, tc, fqTable, false, constants.Add, time.Now().UTC(), cols...)
	assert.Error(d.T(), err)
}

func (d *DDLTestSuite) TestAlterTableAdd() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
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
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	err := ddl.AlterTable(d.ctx, d.snowflakeStore, tc, fqTable, false, constants.Add, time.Now().UTC(), cols...)
	assert.Equal(d.T(), len(cols), d.fakeSnowflakeStore.ExecCallCount(), "called SFLK the same amt to create cols")
	assert.NoError(d.T(), err)

	// Check the table config
	tableConfig := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)
	for col, kind := range tableConfig.Columns() {
		var found bool
		for _, expCol := range cols {
			if found = col == expCol.Name; found {
				assert.Equal(d.T(), kind, expCol.Kind, fmt.Sprintf("wrong col kind, col: %s", col))
				break
			}
		}

		assert.True(d.T(), found,
			fmt.Sprintf("Col not found: %s, actual list: %v, expected list: %v",
				col, tableConfig.Columns(), cols))
	}
}

func (d *DDLTestSuite) TestAlterTableDeleteDryRun() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
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
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, nil, false, true))
	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	err := ddl.AlterTable(d.ctx, d.snowflakeStore, tc, fqTable, false, constants.Delete, time.Now().UTC(), cols...)
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
	err = ddl.AlterTable(d.ctx, d.snowflakeStore, tableConfig, fqTable, false, constants.Delete, time.Now().UTC(), cols...)
	assert.Nil(d.T(), err)
	assert.Equal(d.T(), 1, d.fakeSnowflakeStore.ExecCallCount(), "tried to delete one column")
	execArg, _ := d.fakeSnowflakeStore.ExecArgsForCall(0)
	assert.Equal(d.T(), execArg, fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTable, constants.Delete, colToActuallyDelete))
}

func (d *DDLTestSuite) TestAlterTableDelete() {
	// Test adding a bunch of columns
	cols := []typing.Column{
		{
			Name: "created_at",
			Kind: typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
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

	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(map[string]typing.KindDetails{}, map[string]time.Time{
		"col_to_delete": time.Now().Add(-2 * constants.DeletionConfidencePadding),
		"answers":       time.Now().Add(-2 * constants.DeletionConfidencePadding),
	}, false, true))

	tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

	err := ddl.AlterTable(d.ctx, d.snowflakeStore, tc, fqTable, false, constants.Delete, time.Now(), cols...)
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
