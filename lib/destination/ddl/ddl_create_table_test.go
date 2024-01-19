package ddl_test

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) Test_CreateTable() {
	fqName := "mock_dataset.mock_table"
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))

	type dwhToTableConfig struct {
		_dwh         destination.DataWarehouse
		_tableConfig *types.DwhTableConfig
		_fakeStore   *mocks.FakeStore
	}

	bigQueryTc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)

	for _, dwhTc := range []dwhToTableConfig{
		{
			_dwh:         d.bigQueryStore,
			_tableConfig: bigQueryTc,
			_fakeStore:   d.fakeBigQueryStore,
		},
		{
			_dwh:         d.snowflakeStagesStore,
			_tableConfig: snowflakeStagesTc,
			_fakeStore:   d.fakeSnowflakeStagesStore,
		},
	} {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               dwhTc._dwh,
			Tc:                dwhTc._tableConfig,
			FqTableName:       fqName,
			CreateTable:       dwhTc._tableConfig.CreateTable(),
			ColumnOp:          constants.Add,
			UppercaseEscNames: ptr.ToBool(false),
		}

		err := ddl.AlterTable(d.ctx, alterTableArgs, columns.NewColumn("name", typing.String))
		assert.Equal(d.T(), 1, dwhTc._fakeStore.ExecCallCount())

		query, _ := dwhTc._fakeStore.ExecArgsForCall(0)
		assert.Equal(d.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", fqName), query)
		assert.NoError(d.T(), err, err)
		assert.Equal(d.T(), false, dwhTc._tableConfig.CreateTable())
	}
}

func (d *DDLTestSuite) TestCreateTable() {
	type _testCase struct {
		name string
		cols []columns.Column

		expectedQuery string
	}

	var (
		happyPathCols = []columns.Column{
			columns.NewColumn("user_id", typing.String),
		}
		twoCols = []columns.Column{
			columns.NewColumn("user_id", typing.String),
			columns.NewColumn("enabled", typing.Boolean),
		}
		bunchOfCols = []columns.Column{
			columns.NewColumn("user_id", typing.String),
			columns.NewColumn("enabled_boolean", typing.Boolean),
			columns.NewColumn("array", typing.Array),
			columns.NewColumn("struct", typing.Struct),
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
		fqTable := "demo.public.experiments"
		d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqTable, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
		tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqTable)

		alterTableArgs := ddl.AlterTableArgs{
			Dwh:               d.snowflakeStagesStore,
			Tc:                tc,
			FqTableName:       fqTable,
			CreateTable:       tc.CreateTable(),
			ColumnOp:          constants.Add,
			CdcTime:           time.Now().UTC(),
			UppercaseEscNames: ptr.ToBool(false),
		}

		err := ddl.AlterTable(d.ctx, alterTableArgs, testCase.cols...)
		assert.NoError(d.T(), err, testCase.name)

		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(index)
		assert.Equal(d.T(), testCase.expectedQuery, execQuery, testCase.name)

		// Check if the table is now marked as created where `CreateTable = false`.
		assert.Equal(d.T(), d.snowflakeStagesStore.GetConfigMap().TableConfig(fqTable).CreateTable(),
			false, d.snowflakeStagesStore.GetConfigMap().TableConfig(fqTable), testCase.name)
	}
}
