package ddl_test

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) Test_CreateTable() {
	bqTableID := bigquery.NewTableIdentifier("", "mock_dataset", "mock_table")
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqTableID, types.NewDwhTableConfig(nil, true))

	snowflakeTableID := snowflake.NewTableIdentifier("", "mock_dataset", "mock_table")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeTableID, types.NewDwhTableConfig(nil, true))

	type dwhToTableConfig struct {
		_tableID       sql.TableIdentifier
		_dwh           destination.DataWarehouse
		_tableConfig   *types.DwhTableConfig
		_fakeStore     *mocks.FakeStore
		_expectedQuery string
	}

	bigQueryTc := d.bigQueryStore.GetConfigMap().TableConfigCache(bqTableID)
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(snowflakeTableID)

	for _, dwhTc := range []dwhToTableConfig{
		{
			_tableID:       bqTableID,
			_dwh:           d.bigQueryStore,
			_tableConfig:   bigQueryTc,
			_fakeStore:     d.fakeBigQueryStore,
			_expectedQuery: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`name` string)", bqTableID.FullyQualifiedName()),
		},
		{
			_tableID:       snowflakeTableID,
			_dwh:           d.snowflakeStagesStore,
			_tableConfig:   snowflakeStagesTc,
			_fakeStore:     d.fakeSnowflakeStagesStore,
			_expectedQuery: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ("NAME" string)`, snowflakeTableID.FullyQualifiedName()),
		},
	} {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     dwhTc._dwh.Dialect(),
			Tc:          dwhTc._tableConfig,
			TableID:     dwhTc._tableID,
			CreateTable: dwhTc._tableConfig.CreateTable(),
			ColumnOp:    constants.Add,
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(dwhTc._dwh, columns.NewColumn("name", typing.String)))
		assert.Equal(d.T(), 1, dwhTc._fakeStore.ExecCallCount())

		query, _ := dwhTc._fakeStore.ExecArgsForCall(0)
		assert.Equal(d.T(), dwhTc._expectedQuery, query)
		assert.False(d.T(), dwhTc._tableConfig.CreateTable())
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
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" ("USER_ID" string)`,
		},
		{
			name:          "happy path + enabled",
			cols:          twoCols,
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" ("USER_ID" string,"ENABLED" boolean)`,
		},
		{
			name:          "complex table creation",
			cols:          bunchOfCols,
			expectedQuery: `CREATE TABLE IF NOT EXISTS demo.public."EXPERIMENTS" ("USER_ID" string,"ENABLED_BOOLEAN" boolean,"ARRAY" array,"STRUCT" variant)`,
		},
	}

	for index, testCase := range testCases {
		tableID := snowflake.NewTableIdentifier("demo", "public", "experiments")
		d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
		tc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)

		alterTableArgs := ddl.AlterTableArgs{
			Dialect:     d.snowflakeStagesStore.Dialect(),
			Tc:          tc,
			TableID:     tableID,
			CreateTable: tc.CreateTable(),
			ColumnOp:    constants.Add,
			CdcTime:     time.Now().UTC(),
			Mode:        config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, testCase.cols...), testCase.name)

		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(index)
		assert.Equal(d.T(), testCase.expectedQuery, execQuery, testCase.name)

		// Check if the table is now marked as created where `CreateTable = false`.
		assert.Equal(d.T(), d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID).CreateTable(),
			false, d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID), testCase.name)
	}
}
