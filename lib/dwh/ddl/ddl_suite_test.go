package ddl_test // to avoid go import cycles.

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/clients/redshift"

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
	bqCtx             context.Context
	fakeBigQueryStore *mocks.FakeStore
	bigQueryStore     *bigquery.Store

	fakeSnowflakeStagesStore *mocks.FakeStore
	snowflakeStagesStore     *snowflake.Store

	fakeRedshiftStore *mocks.FakeStore
	redshiftStore     *redshift.Store
}

func (d *DDLTestSuite) SetupTest() {
	ctx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
	})

	bqCtx := config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
		Config: &config.Config{
			BigQuery: &config.BigQuery{
				ProjectID: "artie-project",
			},
		},
	})

	bqCtx = logger.InjectLoggerIntoCtx(bqCtx)
	ctx = logger.InjectLoggerIntoCtx(ctx)
	d.ctx = ctx
	d.bqCtx = bqCtx

	d.fakeBigQueryStore = &mocks.FakeStore{}
	bqStore := db.Store(d.fakeBigQueryStore)
	d.bigQueryStore = bigquery.LoadBigQuery(ctx, &bqStore)

	d.fakeSnowflakeStagesStore = &mocks.FakeStore{}
	snowflakeStagesStore := db.Store(d.fakeSnowflakeStagesStore)
	d.snowflakeStagesStore = snowflake.LoadSnowflake(ctx, &snowflakeStagesStore)

	d.fakeRedshiftStore = &mocks.FakeStore{}
	redshiftStore := db.Store(d.fakeRedshiftStore)
	d.redshiftStore = redshift.LoadRedshift(ctx, &redshiftStore)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
