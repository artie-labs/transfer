package ddl_test

import (
	"time"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestValidate_AlterTableArgs() {
	a := &ddl.AlterTableArgs{
		ColumnOp:    constants.Delete,
		CreateTable: true,
		Mode:        config.Replication,
	}
	assert.ErrorContains(d.T(), a.Validate(), "dialect cannot be nil")

	a.Dialect = bigQueryDialect.BigQueryDialect{}
	assert.ErrorContains(d.T(), a.Validate(), "incompatible operation - cannot drop columns and create table at the same time")

	a.ColumnOp = constants.Add
	a.CreateTable = false
	a.TemporaryTable = true
	assert.ErrorContains(d.T(), a.Validate(), "incompatible operation - we should not be altering temporary tables, only create")
}

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
