package ddl_test // to avoid go import cycles.

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"

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

	fakeSnowflakeStagesStore *mocks.FakeStore
	snowflakeStagesStore     *snowflake.Store
}

func (d *DDLTestSuite) SetupTest() {
	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
	})

	ctx = logger.InjectLoggerIntoCtx(ctx)
	d.ctx = ctx

	d.fakeBigQueryStore = &mocks.FakeStore{}
	bqStore := db.Store(d.fakeBigQueryStore)
	d.bigQueryStore = bigquery.LoadBigQuery(ctx, &bqStore)

	d.fakeSnowflakeStore = &mocks.FakeStore{}
	sflkStore := db.Store(d.fakeSnowflakeStore)
	d.snowflakeStore = snowflake.LoadSnowflake(ctx, &sflkStore, false)

	d.fakeSnowflakeStagesStore = &mocks.FakeStore{}
	snowflakeStagesStore := db.Store(d.fakeSnowflakeStagesStore)
	d.snowflakeStagesStore = snowflake.LoadSnowflake(ctx, &snowflakeStagesStore, true)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
