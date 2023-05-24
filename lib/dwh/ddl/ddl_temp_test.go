package ddl_test

import (
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) TestCreateTemporaryTable_Errors() {
	fqName := "mock_dataset.mock_table"
	d.snowflakeStore.GetConfigMap().AddTableToConfig(fqName, types.NewDwhTableConfig(typing.Columns{}, nil, true, true))
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
}

func (d *DDLTestSuite) TestCreateTemporaryTable() {

}
