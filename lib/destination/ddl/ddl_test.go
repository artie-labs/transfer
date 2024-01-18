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
		"gGghHH",
	}

	for _, dest := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if dest.Label() == constants.Snowflake {
			fakeStore = d.fakeSnowflakeStagesStore
		} else if dest.Label() == constants.BigQuery {
			fakeStore = d.fakeBigQueryStore
		}

		for tableIndex, table := range tablesToDrop {
			fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
			_ = ddl.DropTemporaryTable(d.ctx, dest, fullTableName, false)

			// There should be the same number of DROP table calls as the number of tables to drop.
			assert.Equal(d.T(), tableIndex+1, fakeStore.ExecCallCount())
			query, _ := fakeStore.ExecArgsForCall(tableIndex)

			if dest.Label() == constants.BigQuery {
				// BigQuery should be case-sensitive.
				assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName), query)
			} else {
				assert.Equal(d.T(), fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.ToLower(fullTableName)), query)
			}
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
		_ = ddl.DropTemporaryTable(d.ctx, d.snowflakeStagesStore, table, false)
		assert.Equal(d.T(), 0, d.fakeSnowflakeStagesStore.ExecCallCount())
	}

	for _, _dwh := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if _dwh.Label() == constants.Snowflake {
			fakeStore = d.fakeSnowflakeStagesStore
		} else if _dwh.Label() == constants.BigQuery {
			fakeStore = d.fakeBigQueryStore
		}

		for _, doNotDropTable := range doNotDropTables {
			_ = ddl.DropTemporaryTable(d.ctx, _dwh, doNotDropTable, false)

			assert.Equal(d.T(), 0, fakeStore.ExecCallCount())
		}

		for index, table := range doNotDropTables {
			fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
			_ = ddl.DropTemporaryTable(d.ctx, _dwh, fullTableName, false)

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
	for _, _dwh := range []destination.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if _dwh.Label() == constants.Snowflake {
			fakeStore = d.fakeSnowflakeStagesStore
			d.fakeSnowflakeStagesStore.ExecReturns(nil, randomErr)
		} else if _dwh.Label() == constants.BigQuery {
			fakeStore = d.fakeBigQueryStore
			d.fakeBigQueryStore.ExecReturns(nil, randomErr)
		}

		var count int
		for _, shouldReturnErr := range []bool{true, false} {
			for _, table := range tablesToDrop {
				fullTableName := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
				err := ddl.DropTemporaryTable(d.ctx, _dwh, fullTableName, shouldReturnErr)
				if shouldReturnErr {
					assert.Error(d.T(), err)
					assert.Contains(d.T(), err.Error(), randomErr.Error())
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
