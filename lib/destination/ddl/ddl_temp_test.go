package ddl_test

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestCreateTemporaryTable_Errors() {
	tableID := dialect.NewTableIdentifier("", "mock_dataset", "mock_table")
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	snowflakeTc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	args := ddl.AlterTableArgs{
		Dialect:        d.snowflakeStagesStore.Dialect(),
		Tc:             snowflakeTc,
		TableID:        tableID,
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
		CdcTime:        time.Time{},
		Mode:           config.Replication,
	}

	// No columns.
	assert.NoError(d.T(), args.AlterTable(d.snowflakeStagesStore))

	args.ColumnOp = constants.Delete
	assert.ErrorContains(d.T(), args.AlterTable(d.snowflakeStagesStore), "incompatible operation - cannot drop columns and create table at the same time")

	// Change it to SFLK + Stage
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
	args.Dialect = d.snowflakeStagesStore.Dialect()
	args.Tc = snowflakeStagesTc
	args.CreateTable = false

	assert.ErrorContains(d.T(), args.AlterTable(d.snowflakeStagesStore), "incompatible operation - we should not be altering temporary tables, only create")
}

func (d *DDLTestSuite) TestCreateTemporaryTable() {
	{
		// Snowflake Stage
		tableID := dialect.NewTableIdentifier("db", "schema", "tempTableName")
		d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
		sflkStageTc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(tableID)
		args := ddl.AlterTableArgs{
			Dialect:        d.snowflakeStagesStore.Dialect(),
			Tc:             sflkStageTc,
			TableID:        tableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			CdcTime:        time.Time{},
			Mode:           config.Replication,
		}

		assert.NoError(d.T(), args.AlterTable(d.snowflakeStagesStore, columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("start", typing.String)))
		assert.Equal(d.T(), 1, d.fakeSnowflakeStagesStore.ExecCallCount())
		query, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(0)

		assert.Contains(d.T(),
			query,
			`CREATE TABLE IF NOT EXISTS db.schema."TEMPTABLENAME" ("FOO" string,"BAR" float,"START" string) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
			query)
	}
	{
		// BigQuery
		tableID := bigQueryDialect.NewTableIdentifier("db", "schema", "tempTableName")
		d.bigQueryStore.GetConfigMap().AddTableToConfig(tableID, types.NewDwhTableConfig(nil, true))
		bqTc := d.bigQueryStore.GetConfigMap().TableConfigCache(tableID)
		args := ddl.AlterTableArgs{
			Dialect:        d.bigQueryStore.Dialect(),
			Tc:             bqTc,
			TableID:        tableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			CdcTime:        time.Time{},
			Mode:           config.Replication,
		}

		assert.NoError(d.T(), args.AlterTable(d.bigQueryStore, columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("select", typing.String)))
		assert.Equal(d.T(), 1, d.fakeBigQueryStore.ExecCallCount())
		bqQuery, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
		// Cutting off the expiration_timestamp since it's time based.
		assert.Contains(d.T(), bqQuery, "CREATE TABLE IF NOT EXISTS `db`.`schema`.`tempTableName` (`foo` string,`bar` float64,`select` string) OPTIONS (expiration_timestamp =")
	}
}

func (d *DDLTestSuite) Test_DropTemporaryTableCaseSensitive() {
	tablesToDrop := []string{
		"foo",
		"abcdef",
		"gghh",
	}

	for i, dest := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if i == 0 {
			fakeStore = d.fakeBigQueryStore
		} else {
			fakeStore = d.fakeSnowflakeStagesStore
		}

		for tableIndex, table := range tablesToDrop {
			tableIdentifier := dest.IdentifierFor(kafkalib.TopicConfig{}, fmt.Sprintf("%s_%s", table, constants.ArtiePrefix))
			_ = ddl.DropTemporaryTable(dest, tableIdentifier, false)

			// There should be the same number of DROP table calls as the number of tables to drop.
			assert.Equal(d.T(), tableIndex+1, fakeStore.ExecCallCount())
			query, _ := fakeStore.ExecArgsForCall(tableIndex)
			assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdentifier.FullyQualifiedName()), query)
		}
	}
}

func (d *DDLTestSuite) Test_DropTemporaryTable() {
	doNotDropTables := []string{
		"foo",
		"bar",
		"abcd",
		"customers.customers",
	}

	// Should not drop since these do not have Artie prefix in the name.
	for _, table := range doNotDropTables {
		tableID := d.bigQueryStore.IdentifierFor(kafkalib.TopicConfig{}, table)
		_ = ddl.DropTemporaryTable(d.snowflakeStagesStore, tableID, false)
		assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecCallCount())
	}

	for i, _dwh := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if i == 0 {
			fakeStore = d.fakeBigQueryStore
		} else {
			fakeStore = d.fakeSnowflakeStagesStore

		}

		for _, doNotDropTable := range doNotDropTables {
			doNotDropTableID := d.bigQueryStore.IdentifierFor(kafkalib.TopicConfig{}, doNotDropTable)
			_ = ddl.DropTemporaryTable(_dwh, doNotDropTableID, false)

			assert.Equal(d.T(), 0, fakeStore.ExecCallCount())
		}

		for index, table := range doNotDropTables {
			fullTableID := d.bigQueryStore.IdentifierFor(kafkalib.TopicConfig{}, fmt.Sprintf("%s_%s", table, constants.ArtiePrefix))
			_ = ddl.DropTemporaryTable(_dwh, fullTableID, false)

			count := index + 1
			assert.Equal(d.T(), count, fakeStore.ExecCallCount())

			query, _ := fakeStore.ExecArgsForCall(index)
			assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableID.FullyQualifiedName()), query)
		}
	}
}

func (d *DDLTestSuite) Test_DropTemporaryTable_Errors() {
	tablesToDrop := []string{
		"foo",
		"bar",
		"abcd",
		"customers.customers",
	}

	randomErr := fmt.Errorf("random err")
	for i, _dwh := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if i == 0 {
			fakeStore = d.fakeBigQueryStore
			d.fakeBigQueryStore.ExecReturns(nil, randomErr)
		} else {
			fakeStore = d.fakeSnowflakeStagesStore
			d.fakeSnowflakeStagesStore.ExecReturns(nil, randomErr)
		}

		var count int
		for _, shouldReturnErr := range []bool{true, false} {
			for _, table := range tablesToDrop {
				tableID := d.bigQueryStore.IdentifierFor(kafkalib.TopicConfig{}, fmt.Sprintf("%s_%s", table, constants.ArtiePrefix))
				err := ddl.DropTemporaryTable(_dwh, tableID, shouldReturnErr)
				if shouldReturnErr {
					assert.ErrorContains(d.T(), err, randomErr.Error())
				} else {
					assert.NoError(d.T(), err)
				}

				count += 1
				assert.Equal(d.T(), count, fakeStore.ExecCallCount())
				query, _ := fakeStore.ExecArgsForCall(count - 1)
				assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableID.FullyQualifiedName()), query)
			}
		}

	}
}
