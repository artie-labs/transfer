package ddl_test

import (
	"fmt"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestCreateTemporaryTable() {
	{
		// Snowflake Stage
		tableID := dialect.NewTableIdentifier("db", "schema", "tempTableName")
		query, err := ddl.BuildCreateTableSQL(config.SharedDestinationColumnSettings{}, d.snowflakeStagesStore.Dialect(), tableID, true, config.Replication, []columns.Column{columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("start", typing.String)})
		assert.NoError(d.T(), err)
		assert.Equal(d.T(), query, `CREATE TABLE IF NOT EXISTS "DB"."SCHEMA"."TEMPTABLENAME" ("FOO" string,"BAR" float,"START" string) DATA_RETENTION_TIME_IN_DAYS = 0 STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`)
	}
	{
		// BigQuery
		tableID := bigQueryDialect.NewTableIdentifier("db", "schema", "tempTableName")
		query, err := ddl.BuildCreateTableSQL(config.SharedDestinationColumnSettings{}, d.bigQueryStore.Dialect(), tableID, true, config.Replication, []columns.Column{columns.NewColumn("foo", typing.String), columns.NewColumn("bar", typing.Float), columns.NewColumn("select", typing.String)})
		assert.NoError(d.T(), err)
		// Cutting off the expiration_timestamp since it's time based.
		assert.Contains(d.T(), query, "CREATE TABLE IF NOT EXISTS `db`.`schema`.`tempTableName` (`foo` string,`bar` float64,`select` string) OPTIONS (expiration_timestamp =", query)
	}
}

func (d *DDLTestSuite) Test_DropTemporaryTableCaseSensitive() {
	tablesToDrop := []string{
		"foo",
		"abcdef",
		"gghh",
	}

	for i, dest := range []destination.Destination{d.bigQueryStore, d.snowflakeStagesStore} {
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

	for i, _dwh := range []destination.Destination{d.bigQueryStore, d.snowflakeStagesStore} {
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
	for i, _dwh := range []destination.Destination{d.bigQueryStore, d.snowflakeStagesStore} {
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
