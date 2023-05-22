package ddl_test // to avoid go import cycles.

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type DDLTestSuite struct {
	suite.Suite
	ctx               context.Context
	fakeBigQueryStore *mocks.FakeStore
	bigQueryStore     *bigquery.Store

	fakeSnowflakeStore *mocks.FakeStore
	snowflakeStore     *snowflake.Store
}

func (d *DDLTestSuite) SetupTest() {
	ctx := context.Background()
	d.ctx = ctx

	d.fakeBigQueryStore = &mocks.FakeStore{}
	bqStore := db.Store(d.fakeBigQueryStore)
	d.bigQueryStore = bigquery.LoadBigQuery(ctx, &bqStore)

	d.fakeSnowflakeStore = &mocks.FakeStore{}
	sflkStore := db.Store(d.fakeSnowflakeStore)
	d.snowflakeStore = snowflake.LoadSnowflake(ctx, &sflkStore, false)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
