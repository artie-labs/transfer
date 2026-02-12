package bigquery

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
)

type BigQueryTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *Store
}

func (b *BigQueryTestSuite) SetupTest() {
	cfg := config.Config{
		BigQuery: &config.BigQuery{
			ProjectID: "artie",
		},
	}

	b.fakeStore = &mocks.FakeStore{}
	store := db.Store(b.fakeStore)
	var err error
	b.store, err = LoadStore(b.T().Context(), cfg, &store)
	assert.NoError(b.T(), err)
}

func TestBigQueryTestSuite(t *testing.T) {
	suite.Run(t, new(BigQueryTestSuite))
}
