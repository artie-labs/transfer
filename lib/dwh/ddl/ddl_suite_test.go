package ddl_test // to avoid go import cycles.

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type DDLTestSuite struct {
	suite.Suite
	fakeBigQueryStore *mocks.FakeStore
	bigQueryStore     *bigquery.Store
}

func (d *DDLTestSuite) SetupTest() {
	ctx := context.Background()
	d.fakeBigQueryStore = &mocks.FakeStore{}
	store := db.Store(d.fakeBigQueryStore)
	d.bigQueryStore = bigquery.LoadBigQuery(ctx, &store)
}

func TestDDLTestSuite(t *testing.T) {
	suite.Run(t, new(DDLTestSuite))
}
