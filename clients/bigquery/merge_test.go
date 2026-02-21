package bigquery

import (
	"fmt"
	"testing"

	bigqueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (b *BigQueryTestSuite) TestBackfillColumn() {
	tableID := bigqueryDialect.NewTableIdentifier("db", "public", "tableName")
	{
		// Test column without default value
		col := columns.NewColumn("foo", typing.Invalid)
		assert.NoError(b.T(), shared.BackfillColumn(b.T().Context(), b.store, col, tableID))
		assert.Equal(b.T(), 0, b.fakeStore.ExecContextCallCount())
	}
	{
		// Test column with default value but already backfilled
		col := columns.NewColumn("foo", typing.Boolean)
		col.SetDefaultValue(true)
		col.SetBackfilled(true)

		assert.NoError(b.T(), shared.BackfillColumn(b.T().Context(), b.store, col, tableID))
		assert.Equal(b.T(), 0, b.fakeStore.ExecContextCallCount())
	}
	{
		// Test boolean column that needs backfilling
		col := columns.NewColumn("foo", typing.Boolean)
		col.SetDefaultValue(true)

		assert.NoError(b.T(), shared.BackfillColumn(b.T().Context(), b.store, col, tableID))

		_, backfillSQL, _ := b.fakeStore.ExecContextArgsForCall(0)
		assert.Equal(b.T(), "UPDATE `db`.`public`.`tableName` as t SET t.`foo` = true WHERE t.`foo` IS NULL;", backfillSQL)

		_, commentSQL, _ := b.fakeStore.ExecContextArgsForCall(1)
		assert.Equal(b.T(), "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo` SET OPTIONS (description=`{\"backfilled\": true}`);", commentSQL)
	}
	{
		// Test string column that needs backfilling
		col := columns.NewColumn("foo2", typing.String)
		col.SetDefaultValue("hello there")

		assert.NoError(b.T(), shared.BackfillColumn(b.T().Context(), b.store, col, tableID))

		_, backfillSQL, _ := b.fakeStore.ExecContextArgsForCall(2)
		assert.Equal(b.T(), "UPDATE `db`.`public`.`tableName` as t SET t.`foo2` = 'hello there' WHERE t.`foo2` IS NULL;", backfillSQL)

		_, commentSQL, _ := b.fakeStore.ExecContextArgsForCall(3)
		assert.Equal(b.T(), "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo2` SET OPTIONS (description=`{\"backfilled\": true}`);", commentSQL)
	}
	{
		// Test numeric column that needs backfilling
		col := columns.NewColumn("foo3", typing.Float)
		col.SetDefaultValue(3.5)

		assert.NoError(b.T(), shared.BackfillColumn(b.T().Context(), b.store, col, tableID))

		_, backfillSQL, _ := b.fakeStore.ExecContextArgsForCall(4)
		assert.Equal(b.T(), "UPDATE `db`.`public`.`tableName` as t SET t.`foo3` = 3.5 WHERE t.`foo3` IS NULL;", backfillSQL)

		_, commentSQL, _ := b.fakeStore.ExecContextArgsForCall(5)
		assert.Equal(b.T(), "ALTER TABLE `db`.`public`.`tableName` ALTER COLUMN `foo3` SET OPTIONS (description=`{\"backfilled\": true}`);", commentSQL)
	}
}

func TestBuildPrefixStatements(t *testing.T) {
	{
		// No reservation configured
		store := &Store{config: config.Config{BigQuery: &config.BigQuery{ProjectID: "artie"}}}
		assert.Empty(t, store.buildPrefixStatements())
	}
	{
		// Reservation configured
		store := &Store{config: config.Config{BigQuery: &config.BigQuery{
			ProjectID:   "artie",
			Reservation: "projects/bq-admin-project-473214/locations/US/reservations/pump-bq-batch",
		}}}
		assert.Equal(t,
			[]string{"SET @@reservation = 'projects/bq-admin-project-473214/locations/US/reservations/pump-bq-batch'"},
			store.buildPrefixStatements(),
		)
	}
}

func (b *BigQueryTestSuite) TestGenerateMergeString() {
	bqSettings := &partition.BigQuerySettings{
		PartitionType:  "time",
		PartitionField: "created_at",
		PartitionBy:    "daily",
	}

	dialect := bigqueryDialect.BigQueryDialect{}

	{
		// nil
		_, err := generateMergeString(bqSettings, dialect, nil)
		assert.ErrorContains(b.T(), err, "values cannot be empty")

		// empty values
		_, err = generateMergeString(bqSettings, dialect, []string{})
		assert.ErrorContains(b.T(), err, "values cannot be empty")
	}
	{
		// Valid
		mergeString, err := generateMergeString(bqSettings, dialect, []string{"2020-01-01"})
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), fmt.Sprintf("DATE(%s.`created_at`) IN ('2020-01-01')", constants.TargetAlias), mergeString)
	}
	{
		// Valid multiple values
		mergeString, err := generateMergeString(bqSettings, dialect, []string{"2020-01-01", "2020-01-02"})
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), fmt.Sprintf("DATE(%s.`created_at`) IN ('2020-01-01','2020-01-02')", constants.TargetAlias), mergeString)
	}
}
