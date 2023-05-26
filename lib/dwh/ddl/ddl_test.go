package ddl_test

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func (d *DDLTestSuite) Test_DropTemporaryTable() {
	doNotDropTables := []string{
		"foo",
		"bar",
		"abcd",
		"customers.customers",
	}

	// Should not do anything to Snowflake since it's not supported.
	for _, table := range doNotDropTables {
		tableWithSuffix := fmt.Sprintf("%s_%s", table, constants.ArtiePrefix)
		_ = ddl.DropTemporaryTable(d.ctx, d.snowflakeStore, table, false)
		_ = ddl.DropTemporaryTable(d.ctx, d.snowflakeStore, tableWithSuffix, false)
		assert.Equal(d.T(), 0, d.fakeSnowflakeStore.ExecCallCount())
	}

	for _, _dwh := range []dwh.DataWarehouse{d.bigQueryStore, d.snowflakeStagesStore} {
		var fakeStore *mocks.FakeStore
		if _dwh.Label() == constants.SnowflakeStages {
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
