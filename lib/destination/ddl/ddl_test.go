package ddl_test

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
)

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
			fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
			_ = ddl.DropTemporaryTable(dest, fullTableName, false)

			// There should be the same number of DROP table calls as the number of tables to drop.
			assert.Equal(d.T(), tableIndex+1, fakeStore.ExecCallCount())
			query, _ := fakeStore.ExecArgsForCall(tableIndex)
			assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.ToLower(fullTableName)), query)
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
		_ = ddl.DropTemporaryTable(d.snowflakeStagesStore, table, false)
		assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecCallCount())
	}

	for i, _dwh := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if i == 0 {
			fakeStore = d.fakeSnowflakeStagesStore
		} else {
			fakeStore = d.fakeBigQueryStore
		}

		for _, doNotDropTable := range doNotDropTables {
			_ = ddl.DropTemporaryTable(_dwh, doNotDropTable, false)

			assert.Equal(d.T(), 0, fakeStore.ExecCallCount())
		}

		for index, table := range doNotDropTables {
			fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
			_ = ddl.DropTemporaryTable(_dwh, fullTableName, false)

			count := index + 1
			assert.Equal(d.T(), count, fakeStore.ExecCallCount())

			query, _ := fakeStore.ExecArgsForCall(index)
			assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName), query)
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
				fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
				err := ddl.DropTemporaryTable(_dwh, fullTableName, shouldReturnErr)
				if shouldReturnErr {
					assert.ErrorContains(d.T(), err, randomErr.Error())
				} else {
					assert.NoError(d.T(), err)
				}

				count += 1
				assert.Equal(d.T(), count, fakeStore.ExecCallCount())
				query, _ := fakeStore.ExecArgsForCall(count - 1)
				assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName), query)
			}
		}

	}
}
