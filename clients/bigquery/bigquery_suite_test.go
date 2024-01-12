package bigquery

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type BigQueryTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *Store
	ctx       context.Context
}

func (b *BigQueryTestSuite) SetupTest() {
	b.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
		Config: &config.Config{
			BigQuery: &config.BigQuery{
				ProjectID: "artie",
			},
		},
	})

	b.fakeStore = &mocks.FakeStore{}
	store := db.Store(b.fakeStore)
	b.store = LoadBigQuery(b.ctx, &store)
}

func TestBigQueryTestSuite(t *testing.T) {
	suite.Run(t, new(BigQueryTestSuite))
}
