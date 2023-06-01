package ddl_test

import (
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) TestValidate_AlterTableArgs() {
	a := &ddl.AlterTableArgs{
		ColumnOp:    constants.Delete,
		CreateTable: true,
	}

	assert.Contains(d.T(), a.Validate().Error(), "incompatible operation - cannot drop columns and create table at the same time")

	a.ColumnOp = constants.Add
	a.CreateTable = false
	a.TemporaryTable = true
	assert.Contains(d.T(), a.Validate().Error(), "incompatible operation - we should not be altering temporary tables, only create")
}

func (d *DDLTestSuite) TestCreateTemporaryTable_Errors() {
	fqName := "mock_dataset.mock_table"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	snowflakeTc := d.snowflakeStore.GetConfigMap().TableConfig(fqName)
	args := ddl.AlterTableArgs{
		Dwh:            d.snowflakeStore,
		Tc:             snowflakeTc,
		FqTableName:    fqName,
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
		CdcTime:        time.Time{},
	}

	err := ddl.AlterTable(d.ctx, args)
	assert.Equal(d.T(), "unexpected dwh: snowflake trying to create a temporary table", err.Error())

	args.ColumnOp = constants.Delete
	err = ddl.AlterTable(d.ctx, args)
	assert.Contains(d.T(), err.Error(), "incompatible operation - cannot drop columns and create table at the same time")

	// Change it to SFLK + Stage
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	snowflakeStagesTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)
	args.Dwh = d.snowflakeStagesStore
	args.Tc = snowflakeStagesTc
	args.CreateTable = false

	err = ddl.AlterTable(d.ctx, args)
	assert.Equal(d.T(), "incompatible operation - we should not be altering temporary tables, only create", err.Error())
}

func (d *DDLTestSuite) TestCreateTemporaryTable() {
	fqName := "db.schema.tempTableName"
	// Snowflake Stage
	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	sflkStageTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(fqName)
	args := ddl.AlterTableArgs{
		Dwh:            d.snowflakeStagesStore,
		Tc:             sflkStageTc,
		FqTableName:    fqName,
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
		CdcTime:        time.Time{},
	}

	err := ddl.AlterTable(d.ctx, args, typing.Column{
		Name:        "foo",
		KindDetails: typing.String,
	}, typing.Column{
		Name:        "bar",
		KindDetails: typing.Float,
	})

	assert.NoError(d.T(), err)
	assert.Equal(d.T(), 1, d.fakeSnowflakeStagesStore.ExecCallCount())
	query, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(0)

	assert.Contains(d.T(),
		query,
		`CREATE TABLE IF NOT EXISTS db.schema.tempTableName (foo string,bar float) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) COMMENT=`,
		query)

	// BigQuery
	d.bigQueryStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(&typing.Columns{}, nil, true, true))
	bqTc := d.bigQueryStore.GetConfigMap().TableConfig(fqName)
	args.Dwh = d.bigQueryStore
	args.Tc = bqTc

	err = ddl.AlterTable(d.ctx, args, typing.Column{
		Name:        "foo",
		KindDetails: typing.String,
	}, typing.Column{
		Name:        "bar",
		KindDetails: typing.Float,
	})
	assert.NoError(d.T(), err)
	assert.Equal(d.T(), 1, d.fakeBigQueryStore.ExecCallCount())
	bqQuery, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
	// Cutting off the expiration_timestamp since it's time based.
	assert.Contains(d.T(), bqQuery, `CREATE TABLE IF NOT EXISTS db.schema.tempTableName (foo string,bar float64) OPTIONS (expiration_timestamp =`)
}
