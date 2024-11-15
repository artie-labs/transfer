package ddl_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

	bqTableID := d.bigQueryStore.IdentifierFor(td.TopicConfig(), td.Name())
	bqName := bqTableID.FullyQualifiedName()

	redshiftTableID := d.redshiftStore.IdentifierFor(td.TopicConfig(), td.Name())
	redshiftName := redshiftTableID.FullyQualifiedName()

	snowflakeTableID := d.snowflakeStagesStore.IdentifierFor(td.TopicConfig(), td.Name())
	snowflakeName := snowflakeTableID.FullyQualifiedName()

	// Testing 3 scenarios here
	// 1. DropDeletedColumns = false, ContainOtherOperations = true, don't delete ever.
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqTableID, types.NewDwhTableConfig(cols.GetColumns(), false))
	bqTc := d.bigQueryStore.GetConfigMap().TableConfigCache(bqTableID)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftTableID, types.NewDwhTableConfig(cols.GetColumns(), false))
	redshiftTc := d.redshiftStore.GetConfigMap().TableConfigCache(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeTableID, types.NewDwhTableConfig(cols.GetColumns(), false))
	snowflakeTc := d.snowflakeStagesStore.GetConfigMap().TableConfigCache(snowflakeTableID)
	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Snowflake
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.snowflakeStagesStore.Dialect(),
			Tc:                     snowflakeTc,
			TableID:                snowflakeTableID,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     bqTc,
			TableID:                bqTableID,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		err := alterTableArgs.AlterTable(d.bigQueryStore, column)
		assert.NoError(d.T(), err)
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.redshiftStore.Dialect(),
			Tc:                     redshiftTc,
			TableID:                redshiftTableID,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.redshiftStore, column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())

	// 2. DropDeletedColumns = true, ContainOtherOperations = false, don't delete ever
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqTableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	bqTc = d.bigQueryStore.GetConfigMap().TableConfigCache(bqTableID)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftTableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	redshiftTc = d.redshiftStore.GetConfigMap().TableConfigCache(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeTableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().TableConfigCache(snowflakeTableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.snowflakeStagesStore.Dialect(),
			Tc:                     snowflakeTc,
			TableID:                snowflakeTableID,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(snowflakeTc.Columns().GetColumns()), snowflakeTc.Columns().GetColumns())

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     bqTc,
			TableID:                bqTableID,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(bqTc.Columns().GetColumns()), bqTc.Columns().GetColumns())

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.redshiftStore.Dialect(),
			Tc:                     redshiftTc,
			TableID:                redshiftTableID,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: false,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.redshiftStore, column))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), originalColumnLength, len(redshiftTc.Columns().GetColumns()), redshiftTc.Columns().GetColumns())

	// 3. DropDeletedColumns = true, ContainOtherOperations = true, drop based on timestamp.
	d.bigQueryStore.GetConfigMap().AddTableToConfig(bqTableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	bqTc = d.bigQueryStore.GetConfigMap().TableConfigCache(bqTableID)

	d.redshiftStore.GetConfigMap().AddTableToConfig(redshiftTableID, types.NewDwhTableConfig(cols.GetColumns(), true))
	redshiftTc = d.redshiftStore.GetConfigMap().TableConfigCache(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTableToConfig(snowflakeTableID, types.NewDwhTableConfig(cols.GetColumns(), true))

	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().TableConfigCache(snowflakeTableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Now, actually try to delete.
	// Snowflake
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.snowflakeStagesStore.Dialect(),
			Tc:                     snowflakeTc,
			TableID:                snowflakeTableID,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, column))
	}

	// BigQuery
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     bqTc,
			TableID:                bqTableID,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))
	}

	// Redshift
	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.redshiftStore.Dialect(),
			Tc:                     redshiftTc,
			TableID:                redshiftTableID,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts,
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.redshiftStore, column))
	}

	// Nothing has been deleted, but it is all added to the permissions table.
	assert.Len(d.T(), bqTc.GetColumns(), originalColumnLength)
	assert.Len(d.T(), redshiftTc.GetColumns(), originalColumnLength)
	assert.Len(d.T(), snowflakeTc.GetColumns(), originalColumnLength)

	assert.Len(d.T(), bqTc.ReadOnlyColumnsToDelete(), originalColumnLength)
	assert.Len(d.T(), redshiftTc.ReadOnlyColumnsToDelete(), originalColumnLength)
	assert.Len(d.T(), snowflakeTc.ReadOnlyColumnsToDelete(), originalColumnLength)

	for _, column := range cols.GetColumns() {
		alterTableArgs := ddl.AlterTableArgs{
			Dialect:                d.snowflakeStagesStore.Dialect(),
			Tc:                     snowflakeTc,
			TableID:                snowflakeTableID,
			CreateTable:            snowflakeTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.snowflakeStagesStore, column))

		// BigQuery
		alterTableArgs = ddl.AlterTableArgs{
			Dialect:                d.bigQueryStore.Dialect(),
			Tc:                     bqTc,
			TableID:                bqTableID,
			CreateTable:            bqTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.bigQueryStore, column))

		// Redshift
		alterTableArgs = ddl.AlterTableArgs{
			Dialect:                d.redshiftStore.Dialect(),
			Tc:                     redshiftTc,
			TableID:                redshiftTableID,
			CreateTable:            redshiftTc.CreateTable(),
			ContainOtherOperations: true,
			ColumnOp:               constants.Delete,
			CdcTime:                ts.Add(2 * constants.DeletionConfidencePadding),
			Mode:                   config.Replication,
		}

		assert.NoError(d.T(), alterTableArgs.AlterTable(d.redshiftStore, column))
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
			if execQuery == fmt.Sprintf(`ALTER TABLE %s drop COLUMN "%s"`, snowflakeName, strings.ToUpper(key)) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	for i := 0; i < d.fakeBigQueryStore.ExecCallCount(); i++ {
		execQuery, _ := d.fakeBigQueryStore.ExecArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf("ALTER TABLE %s drop COLUMN `%s`", bqName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	for i := 0; i < d.fakeRedshiftStore.ExecCallCount(); i++ {
		execQuery, _ := d.fakeRedshiftStore.ExecArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf(`ALTER TABLE %s drop COLUMN "%s"`, redshiftName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}
}
