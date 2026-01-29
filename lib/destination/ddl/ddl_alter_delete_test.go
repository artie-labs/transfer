package ddl_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestAlterDelete_Complete() {
	ts := time.Now()
	allCols := []string{"a", "b", "c", "d"}
	cols := columns.NewColumns(nil)
	for _, colName := range allCols {
		cols.AddColumn(columns.NewColumn(colName, typing.String))
	}

	td := optimization.NewTableData(cols, config.Replication, nil, kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "public",
	}, "tableName")

	originalColumnLength := len(cols.GetColumns())

	bqTableID := d.bigQueryStore.IdentifierFor(td.TopicConfig().BuildDatabaseAndSchemaPair(), td.Name())
	bqName := bqTableID.FullyQualifiedName()

	redshiftTableID := d.redshiftStore.IdentifierFor(td.TopicConfig().BuildDatabaseAndSchemaPair(), td.Name())
	redshiftName := redshiftTableID.FullyQualifiedName()

	snowflakeTableID := d.snowflakeStagesStore.IdentifierFor(td.TopicConfig().BuildDatabaseAndSchemaPair(), td.Name())
	snowflakeName := snowflakeTableID.FullyQualifiedName()

	// Testing 3 scenarios here
	// 1. DropDeletedColumns = false, ContainOtherOperations = true, don't delete ever.
	d.bigQueryStore.GetConfigMap().AddTable(bqTableID, types.NewDestinationTableConfig(cols.GetColumns(), false))
	bqTc := d.bigQueryStore.GetConfigMap().GetTableConfig(bqTableID)

	d.redshiftStore.GetConfigMap().AddTable(redshiftTableID, types.NewDestinationTableConfig(cols.GetColumns(), false))
	redshiftTc := d.redshiftStore.GetConfigMap().GetTableConfig(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTable(snowflakeTableID, types.NewDestinationTableConfig(cols.GetColumns(), false))
	snowflakeTc := d.snowflakeStagesStore.GetConfigMap().GetTableConfig(snowflakeTableID)
	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Snowflake
	for _, column := range cols.GetColumns() {
		err := shared.AlterTableDropColumns(d.T().Context(), d.snowflakeStagesStore, snowflakeTc, snowflakeTableID, []columns.Column{column}, ts, true)
		assert.NoError(d.T(), err)
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), snowflakeTc.GetColumns(), originalColumnLength)

	// BigQuery
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, bqTc, bqTableID, []columns.Column{column}, ts, true))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), bqTc.GetColumns(), originalColumnLength)

	// Redshift
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.redshiftStore, redshiftTc, redshiftTableID, []columns.Column{column}, ts, true))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), redshiftTc.GetColumns(), originalColumnLength)

	// 2. DropDeletedColumns = true, ContainOtherOperations = false, don't delete ever
	d.bigQueryStore.GetConfigMap().AddTable(bqTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	bqTc = d.bigQueryStore.GetConfigMap().GetTableConfig(bqTableID)

	d.redshiftStore.GetConfigMap().AddTable(redshiftTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	redshiftTc = d.redshiftStore.GetConfigMap().GetTableConfig(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTable(snowflakeTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().GetTableConfig(snowflakeTableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.snowflakeStagesStore, snowflakeTc, snowflakeTableID, []columns.Column{column}, ts, false))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), snowflakeTc.GetColumns(), originalColumnLength)

	// BigQuery
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, bqTc, bqTableID, []columns.Column{column}, ts, false))
	}

	// Never actually deleted.
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), bqTc.GetColumns(), originalColumnLength)

	// Redshift
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.redshiftStore, redshiftTc, redshiftTableID, []columns.Column{column}, ts, false))
	}

	// Never actually deleted.
	assert.Empty(d.T(), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Len(d.T(), redshiftTc.GetColumns(), originalColumnLength)

	// 3. DropDeletedColumns = true, ContainOtherOperations = true, drop based on timestamp.
	d.bigQueryStore.GetConfigMap().AddTable(bqTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	bqTc = d.bigQueryStore.GetConfigMap().GetTableConfig(bqTableID)

	d.redshiftStore.GetConfigMap().AddTable(redshiftTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))
	redshiftTc = d.redshiftStore.GetConfigMap().GetTableConfig(redshiftTableID)

	d.snowflakeStagesStore.GetConfigMap().AddTable(snowflakeTableID, types.NewDestinationTableConfig(cols.GetColumns(), true))

	snowflakeTc = d.snowflakeStagesStore.GetConfigMap().GetTableConfig(snowflakeTableID)

	// Prior to deletion, there should be no colsToDelete
	assert.Equal(d.T(), 0, len(bqTc.ReadOnlyColumnsToDelete()), bqTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(redshiftTc.ReadOnlyColumnsToDelete()), redshiftTc.ReadOnlyColumnsToDelete())
	assert.Equal(d.T(), 0, len(snowflakeTc.ReadOnlyColumnsToDelete()), snowflakeTc.ReadOnlyColumnsToDelete())

	// Now, actually try to delete.
	{
		// Snowflake
		for _, column := range cols.GetColumns() {
			assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.snowflakeStagesStore, snowflakeTc, snowflakeTableID, []columns.Column{column}, ts, true))
		}
	}

	// BigQuery
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, bqTc, bqTableID, []columns.Column{column}, ts, true))
	}

	// Redshift
	for _, column := range cols.GetColumns() {
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.redshiftStore, redshiftTc, redshiftTableID, []columns.Column{column}, ts, true))
	}

	// Nothing has been deleted, but it is all added to the permissions table.
	assert.Len(d.T(), bqTc.GetColumns(), originalColumnLength)
	assert.Len(d.T(), redshiftTc.GetColumns(), originalColumnLength)
	assert.Len(d.T(), snowflakeTc.GetColumns(), originalColumnLength)

	assert.Len(d.T(), bqTc.ReadOnlyColumnsToDelete(), originalColumnLength)
	assert.Len(d.T(), redshiftTc.ReadOnlyColumnsToDelete(), originalColumnLength)
	assert.Len(d.T(), snowflakeTc.ReadOnlyColumnsToDelete(), originalColumnLength)

	for _, column := range cols.GetColumns() {
		// Snowflake
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.snowflakeStagesStore, snowflakeTc, snowflakeTableID, []columns.Column{column}, ts.Add(2*constants.DeletionConfidencePadding), true))
		// BigQuery
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.bigQueryStore, bqTc, bqTableID, []columns.Column{column}, ts.Add(2*constants.DeletionConfidencePadding), true))
		// Redshift
		assert.NoError(d.T(), shared.AlterTableDropColumns(d.T().Context(), d.redshiftStore, redshiftTc, redshiftTableID, []columns.Column{column}, ts.Add(2*constants.DeletionConfidencePadding), true))
	}

	// Everything has been deleted.
	assert.Empty(d.T(), snowflakeTc.GetColumns())
	assert.Empty(d.T(), bqTc.GetColumns())
	assert.Empty(d.T(), redshiftTc.GetColumns())

	assert.Empty(d.T(), snowflakeTc.ReadOnlyColumnsToDelete())
	assert.Empty(d.T(), bqTc.ReadOnlyColumnsToDelete())
	assert.Empty(d.T(), redshiftTc.ReadOnlyColumnsToDelete())
	allColsMap := make(map[string]bool, len(allCols))
	for _, allCol := range allCols {
		allColsMap[allCol] = true
	}

	assert.True(d.T(), d.fakeSnowflakeStagesStore.ExecContextCallCount() > 0)
	for i := 0; i < d.fakeSnowflakeStagesStore.ExecContextCallCount(); i++ {
		_, execQuery, _ := d.fakeSnowflakeStagesStore.ExecContextArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf(`ALTER TABLE %s DROP COLUMN IF EXISTS "%s"`, snowflakeName, strings.ToUpper(key)) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	assert.True(d.T(), d.fakeBigQueryStore.ExecContextCallCount() > 0)
	for i := 0; i < d.fakeBigQueryStore.ExecContextCallCount(); i++ {
		_, execQuery, _ := d.fakeBigQueryStore.ExecContextArgsForCall(0)
		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf("ALTER TABLE %s DROP COLUMN `%s`", bqName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}

	assert.True(d.T(), d.fakeRedshiftStore.ExecContextCallCount() > 0)
	for i := 0; i < d.fakeRedshiftStore.ExecContextCallCount(); i++ {
		_, execQuery, _ := d.fakeRedshiftStore.ExecContextArgsForCall(0)

		var found bool
		for key := range allColsMap {
			if execQuery == fmt.Sprintf(`ALTER TABLE %s DROP COLUMN "%s"`, redshiftName, key) {
				found = true
			}
		}

		assert.True(d.T(), found, execQuery)
	}
}
