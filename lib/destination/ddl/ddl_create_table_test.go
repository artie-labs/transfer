package ddl_test

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"

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
	bqTableID := bigquery.NewTableIdentifier("", "mock_dataset", "mock_table")
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqTableID, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))

	snowflakeTableID := snowflake.NewTableIdentifier("", "mock_dataset", "mock_table")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeTableID, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))

	type dwhToTableConfig struct {
		_tableID     types.TableIdentifier
		_dwh         destination.DataWarehouse
		_tableConfig *types.DwhTableConfig
		_fakeStore   *mocks.FakeStore
	}

	bigQueryTc := d.bigQueryStore.GetConfigMap().TableConfig(bqTableID)
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(snowflakeTableID)

	for _, dwhTc := range []dwhToTableConfig{
		{
			_tableID:     bqTableID,
			_dwh:         d.bigQueryStore,
			_tableConfig: bigQueryTc,
			_fakeStore:   d.fakeBigQueryStore,
		},
		{
			_tableID:     snowflakeTableID,
			_dwh:         d.snowflakeStagesStore,
			_tableConfig: snowflakeStagesTc,
			_fakeStore:   d.fakeSnowflakeStagesStore,
		},
	} {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:         dwhTc._dwh,
			Tc:          dwhTc._tableConfig,
			TableID:     dwhTc._tableID,
			CreateTable: dwhTc._tableConfig.CreateTable(),
			ColumnOp:    constants.Add,
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(columns.NewColumn("name", typing.String)))
		assert.Equal(d.T(), 1, dwhTc._fakeStore.ExecCallCount())

		query, _ := dwhTc._fakeStore.ExecArgsForCall(0)
		assert.Equal(d.T(), query, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (name string)", dwhTc._tableID.FullyQualifiedName()), query)
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
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" (user_id string)`,
		},
		{
			name:          "happy path + enabled",
			cols:          twoCols,
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" (user_id string,enabled boolean)`,
		},
		{
			name:          "complex table creation",
			cols:          bunchOfCols,
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" (user_id string,enabled_boolean boolean,array array,struct variant)`,
		},
	}

	for index, testCase := range testCases {
		tableID := snowflake.NewTableIdentifier("demo", "public", "experiments")
		d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
		tc := d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID)

		alterTableArgs := ddl.AlterTableArgs{
			Dwh:         d.snowflakeStagesStore,
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Add,
			CdcTime:     time.Now().UTC(),
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(testCase.cols...), testCase.name)

		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(index)
		assert.Equal(d.T(), testCase.expectedQuery, execQuery, testCase.name)

		// Check if the table is now marked as created where `CreateTable = false`.
		assert.Equal(d.T(), d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID).CreateTable(),
			false, d.snowflakeStagesStore.GetConfigMap().TableConfig(tableID), testCase.name)
	}
}
