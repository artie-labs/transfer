package ddl_test

import (
	"fmt"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (d *DDLTestSuite) TestShouldCreatePrimaryKey() {
	pk := columns.NewColumn("foo", typing.String)
	pk.SetPrimaryKey(true)
	{
		// Primary key check
		{
			// Column is not a primary key
			col := columns.NewColumn("foo", typing.String)
			assert.False(d.T(), ddl.ShouldCreatePrimaryKey(col, config.Replication, true))
		}
		{
			// Column is a primary key
			assert.True(d.T(), ddl.ShouldCreatePrimaryKey(pk, config.Replication, true))
		}
	}
	{
		// False because it's history mode
		assert.False(d.T(), ddl.ShouldCreatePrimaryKey(pk, config.History, true))
	}
	{
		// False because it's not a create table operation
		assert.False(d.T(), ddl.ShouldCreatePrimaryKey(pk, config.Replication, false))
	}
	{
		// True because it's a primary key, replication mode, and create table operation
		assert.True(d.T(), ddl.ShouldCreatePrimaryKey(pk, config.Replication, true))
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
