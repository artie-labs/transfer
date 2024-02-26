package ddl_test // to avoid go import cycles.

import (
	"testing"

	"github.com/artie-labs/transfer/clients/redshift"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type DDLTestSuite struct {
	suite.Suite
	fakeBigQueryStore *mocks.FakeStore
	bigQueryStore     *bigquery.Store
	bigQueryCfg       config.Config

	fakeSnowflakeStagesStore *mocks.FakeStore
	snowflakeStagesStore     *snowflake.Store

	fakeRedshiftStore *mocks.FakeStore
	redshiftStore     *redshift.Store
}

func (d *DDLTestSuite) SetupTest() {
	cfg := config.Config{
		Redshift: &config.Redshift{},
	}

	d.bigQueryCfg = config.Config{
		BigQuery: &config.BigQuery{
			ProjectID: "artie-project",
		},
	}

	d.fakeBigQueryStore = &mocks.FakeStore{}

	d.bigQueryStore = bigquery.LoadBigQuery(d.bigQueryCfg)
	d.bigQueryStore.Store = db.Store(d.fakeBigQueryStore)

	d.fakeSnowflakeStagesStore = &mocks.FakeStore{}
	snowflakeStagesStore := db.Store(d.fakeSnowflakeStagesStore)
	d.snowflakeStagesStore = snowflake.LoadSnowflake(cfg, &snowflakeStagesStore)

	d.fakeRedshiftStore = &mocks.FakeStore{}
	d.redshiftStore = redshift.LoadRedshift(cfg)
	d.redshiftStore.Store = db.Store(d.fakeRedshiftStore)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
