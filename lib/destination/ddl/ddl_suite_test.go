package ddl_test // to avoid go import cycles.

import (
	"testing"

	"github.com/artie-labs/transfer/clients/redshift"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
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
	d.bigQueryCfg = config.Config{
		BigQuery: &config.BigQuery{
			ProjectID: "artie-project",
		},
	}

	d.fakeBigQueryStore = &mocks.FakeStore{}
	bqStore := db.Store(d.fakeBigQueryStore)

	var err error
	d.bigQueryStore, err = bigquery.LoadBigQuery(d.T().Context(), d.bigQueryCfg, &bqStore)
	assert.NoError(d.T(), err)

	d.fakeSnowflakeStagesStore = &mocks.FakeStore{}
	snowflakeStagesStore := db.Store(d.fakeSnowflakeStagesStore)
	snowflakeCfg := config.Config{
		Snowflake: &config.Snowflake{},
	}
	d.snowflakeStagesStore, err = snowflake.LoadSnowflake(d.T().Context(), snowflakeCfg, &snowflakeStagesStore)
	assert.NoError(d.T(), err)

	d.fakeRedshiftStore = &mocks.FakeStore{}
	redshiftStore := db.Store(d.fakeRedshiftStore)
	redshiftCfg := config.Config{Redshift: &config.Redshift{}}
	d.redshiftStore, err = redshift.LoadRedshift(d.T().Context(), redshiftCfg, &redshiftStore)
	assert.NoError(d.T(), err)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
