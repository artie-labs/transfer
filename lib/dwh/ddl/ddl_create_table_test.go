package ddl_test

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) Test_CreateTable() {
	fqName := "mock_dataset.mock_table"
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))

	ctx := context.Background()

	type dwhToTableConfig struct {
		_dwh         dwh.DataWarehouse
		_tableConfig *types.DwhTableConfig
		_fakeStore   *mocks.FakeStore
	}

	bigQueryTc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	snowflakeTc := d.snowflakeStore.GetConfigMap().TableConfig(fqName)
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)

	for _, dwhTc := range []dwhToTableConfig{
		{
			_dwh:         d.bigQueryStore,
			_tableConfig: bigQueryTc,
			_fakeStore:   d.fakeBigQueryStore,
		},
		{
			_dwh:         d.snowflakeStore,
			_tableConfig: snowflakeTc,
			_fakeStore:   d.fakeSnowflakeStore,
		},
		{
			_dwh:         d.snowflakeStagesStore,
			_tableConfig: snowflakeStagesTc,
			_fakeStore:   d.fakeSnowflakeStagesStore,
		},
	} {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:         dwhTc._dwh,
			Tc:          dwhTc._tableConfig,
			FqTableName: fqName,
			CreateTable: dwhTc._tableConfig.CreateTable,
			ColumnOp:    constants.Add,
		}

		err := ddl.AlterTable(ctx, alterTableArgs, typing.Column{Name: "name", KindDetails: typing.String})
		assert.Equal(d.T(), 1, dwhTc._fakeStore.ExecCallCount())

		query, _ := dwhTc._fakeStore.ExecArgsForCall(0)
		assert.Equal(d.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", fqName), query)
		assert.NoError(d.T(), err, err)
		assert.Equal(d.T(), false, dwhTc._tableConfig.CreateTable)
	}
}

func (d *DDLTestSuite) TestCreateTable() {
	type _testCase struct {
		name string
		cols []typing.Column

		expectedQuery string
	}

	var (
		happyPathCols = []typing.Column{
			{
				Name:        "user_id",
				KindDetails: typing.String,
			},
		}
		twoCols = []typing.Column{
			{
				Name:        "user_id",
				KindDetails: typing.String,
			},
			{
				Name:        "enabled",
				KindDetails: typing.Boolean,
			},
		}
		bunchOfCols = []typing.Column{
			{
				Name:        "user_id",
				KindDetails: typing.String,
			},
			{
				Name:        "enabled_boolean",
				KindDetails: typing.Boolean,
			},
			{
				Name:        "array",
				KindDetails: typing.Array,
			},
			{
				Name:        "struct",
				KindDetails: typing.Struct,
			},
		}
	)

	testCases := []_testCase{
		{
			name:          "happy path",
			cols:          happyPathCols,
			expectedQuery: "CREATE TABLE IF NOT EXISTS demo.public.experiments (user_id string)",
		},
		{
			name:          "happy path + enabled",
			cols:          twoCols,
			expectedQuery: "CREATE TABLE IF NOT EXISTS demo.public.experiments (user_id string,enabled boolean)",
		},
		{
			name:          "complex table creation",
			cols:          bunchOfCols,
			expectedQuery: "CREATE TABLE IF NOT EXISTS demo.public.experiments (user_id string,enabled_boolean boolean,array array,struct variant)",
		},
	}

	for index, testCase := range testCases {
		ctx := context.Background()
		fqTable := "demo.public.experiments"
		d.snowflakeStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
		tc := d.snowflakeStore.GetConfigMap().TableConfig(fqTable)

		alterTableArgs := ddl.AlterTableArgs{
			Dwh:         d.snowflakeStore,
			Tc:          tc,
			FqTableName: fqTable,
			CreateTable: tc.CreateTable,
			ColumnOp:    constants.Add,
			CdcTime:     time.Now().UTC(),
		}

		err := ddl.AlterTable(ctx, alterTableArgs, testCase.cols...)
		assert.NoError(d.T(), err, testCase.name)

		execQuery, _ := d.fakeSnowflakeStore.ExecArgsForCall(index)
		assert.Equal(d.T(), testCase.expectedQuery, execQuery, testCase.name)

		// Check if the table is now marked as created where `CreateTable = false`.
		assert.Equal(d.T(), d.snowflakeStore.GetConfigMap().TableConfig(fqTable).CreateTable,
			false, d.snowflakeStore.GetConfigMap().TableConfig(fqTable), testCase.name)
	}
}
