package ddl_test

import (
	"time"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) TestValidate_AlterTableArgs() {
	a := &ddl.AlterTableArgs{
		ColumnOp:          constants.Delete,
		CreateTable:       true,
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	assert.Contains(d.T(), a.Validate().Error(), "incompatible operation - cannot drop columns and create table at the same time")

	a.ColumnOp = constants.Add
	a.CreateTable = false
	a.TemporaryTable = true
	assert.Contains(d.T(), a.Validate().Error(), "incompatible operation - we should not be altering temporary tables, only create")
}

func (d *DDLTestSuite) TestCreateTemporaryTable_Errors() {
	tableID := snowflake.NewTableIdentifier("", "mock_dataset", "mock_table")
	fqName := "mock_dataset.mock_table"
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	snowflakeTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)
	args := ddl.AlterTableArgs{
		Dwh:               d.snowflakeStagesStore,
		Tc:                snowflakeTc,
		TableID:           tableID,
		CreateTable:       true,
		TemporaryTable:    true,
		ColumnOp:          constants.Add,
		CdcTime:           time.Time{},
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	// No columns.
	assert.NoError(d.T(), args.AlterTable())

	args.ColumnOp = constants.Delete
	assert.ErrorContains(d.T(), args.AlterTable(), "incompatible operation - cannot drop columns and create table at the same time")

	// Change it to SFLK + Stage
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)
	args.Dwh = d.snowflakeStagesStore
	args.Tc = snowflakeStagesTc
	args.CreateTable = false

	assert.ErrorContains(d.T(), args.AlterTable(), "incompatible operation - we should not be altering temporary tables, only create")
}

func (d *DDLTestSuite) TestCreateTemporaryTable() {
	tableID := snowflake.NewTableIdentifier("db", "schema", "tempTableName")
	fqName := "db.schema.tempTableName"
	// Snowflake Stage
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	sflkStageTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)
	args := ddl.AlterTableArgs{
		Dwh:               d.snowflakeStagesStore,
		Tc:                sflkStageTc,
		TableID:           tableID,
		CreateTable:       true,
		TemporaryTable:    true,
		ColumnOp:          constants.Add,
		CdcTime:           time.Time{},
		UppercaseEscNames: ptr.ToBool(false),
		Mode:              config.Replication,
	}

	assert.NoError(d.T(), args.AlterTable(columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("start", typing.String)))
	assert.Equal(d.T(), 1, d.fakeSnowflakeStagesStore.ExecCallCount())
	query, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(0)

	assert.Contains(d.T(),
		query,
		`CREATE TABLE IF NOT EXISTS db.schema.tempTableName (foo string,bar float,"start" string) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
		query)

	// BigQuery
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&columns.Columns{}, nil, true, true))
	bqTc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	args.Dwh = d.bigQueryStore
	args.Tc = bqTc

	assert.NoError(d.T(), args.AlterTable(columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("select", typing.String)))
	assert.Equal(d.T(), 1, d.fakeBigQueryStore.ExecCallCount())
	bqQuery, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
	// Cutting off the expiration_timestamp since it's time based.
	assert.Contains(d.T(), bqQuery, "CREATE TABLE IF NOT EXISTS db.schema.tempTableName (foo string,bar float64,`select` string) OPTIONS (expiration_timestamp =")
}
