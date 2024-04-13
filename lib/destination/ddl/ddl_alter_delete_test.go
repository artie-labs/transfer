package ddl_test

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) TestAlterDelete_Complete() {
	ts := time.Now()
	allCols := []string{"a", "b", "c", "d"}
	var cols columns.Columns
	for _, colName := range allCols {
		cols.AddColumn(columns.NewColumn(colName, typing.String))
	}

	td := optimization.NewTableData(&cols, config.Replication, nil, kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "public",
	}, "tableName")

	originalColumnLength := len(cols.GetColumns())
	bqName := d.bigQueryStore.ToFullyQualifiedName(td, true)
	redshiftName := d.redshiftStore.ToFullyQualifiedName(td, true)
	snowflakeName := d.snowflakeStagesStore.ToFullyQualifiedName(td, true)

	// Testing 3 scenarios here
	// 1. DropDeletedColumns = false, ContainOtherOperations = true, don't delete ever.
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqName, types.NewDwhTableConfig(&cols, nil, false, false))
	bqTc := d.bigQueryStore.GetConfigMap().TableConfig(bqName)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftName, types.NewDwhTableConfig(&cols, nil, false, false))
	redshiftTc := d.redshiftStore.GetConfigMap().TableConfig(redshiftName)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeName, types.NewDwhTableConfig(&cols, nil, false, false))
	snowflakeTc := d.snowflakeStagesStore.GetConfigMap().TableConfig(snowflakeName)
	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Snowflake
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.snowflakeStagesStore,
			Tc:                     snowflakeTc,
			FqTableName:            snowflakeName,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     bqTc,
			FqTableName:            bqName,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		err := alterTableArgs.AlterTable(column)
		assert.NoError(d.T(), err)
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.redshiftStore,
			Tc:                     redshiftTc,
			FqTableName:            redshiftName,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())

	// 2. DropDeletedColumns = true, ContainOtherOperations = false, don't delete ever
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqName, types.NewDwhTableConfig(&cols, nil, false, true))
	bqTc = d.bigQueryStore.GetConfigMap().TableConfig(bqName)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftName, types.NewDwhTableConfig(&cols, nil, false, true))
	redshiftTc = d.redshiftStore.GetConfigMap().TableConfig(redshiftName)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeName, types.NewDwhTableConfig(&cols, nil, false, true))
	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().TableConfig(snowflakeName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.snowflakeStagesStore,
			Tc:                     snowflakeTc,
			FqTableName:            snowflakeName,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     bqTc,
			FqTableName:            bqName,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.redshiftStore,
			Tc:                     redshiftTc,
			FqTableName:            redshiftName,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())

	// 3. DropDeletedColumns = true, ContainOtherOperations = true, drop based on timestamp.
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqName, types.NewDwhTableConfig(&cols, nil, false, true))
	bqTc = d.bigQueryStore.GetConfigMap().TableConfig(bqName)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftName, types.NewDwhTableConfig(&cols, nil, false, true))
	redshiftTc = d.redshiftStore.GetConfigMap().TableConfig(redshiftName)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeName, types.NewDwhTableConfig(&cols, nil, false, true))
	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().TableConfig(snowflakeName)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Now, actually try to delete.
	// Snowflake
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.snowflakeStagesStore,
			Tc:                     snowflakeTc,
			FqTableName:            snowflakeName,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     bqTc,
			FqTableName:            bqName,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.redshiftStore,
			Tc:                     redshiftTc,
			FqTableName:            redshiftName,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Nothing has been deleted, but it is all added to the permissions table.
	assert.Equal(d.T(), originalColumnLength, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())

	assert.Equal(d.T(), originalColumnLength, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dwh:                    d.snowflakeStagesStore,
			Tc:                     snowflakeTc,
			FqTableName:            snowflakeName,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))

		// BigQuery
		alterTableArgs = ddl.AlterTableArgs{
			Dwh:                    d.bigQueryStore,
			Tc:                     bqTc,
			FqTableName:            bqName,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))

		// Redshift
		alterTableArgs = ddl.AlterTableArgs{
			Dwh:                    d.redshiftStore,
			Tc:                     redshiftTc,
			FqTableName:            redshiftName,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			UppercaseEscNames:      ptr.ToBool(false),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(column))
	}

	// Everything has been deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())
	assert.Equal(d.T(), 0, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())
	assert.Equal(d.T(), 0, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())

	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())

	allColsMap := make(map[string]bool, len(allCols))
	for _, allCol := range allCols {
		allColsMap[allCol] = true
	}

	for i := 0; i < d.fakeSnowflakeStagesStore.ExecCallCount(); i++ {
		execQuery, _ := d.fakeSnowflakeStagesStore.ExecArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", snowflakeName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	for i := 0; i < d.fakeBigQueryStore.ExecCallCount(); i++ {
		execQuery, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", bqName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	for i := 0; i < d.fakeRedshiftStore.ExecCallCount(); i++ {
		execQuery, _ := d.fakeRedshiftStore.ExecArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf("ALTER TABLE %s drop COLUMN %s", redshiftName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}
}
