package bigquery

import (
	"context"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
	"testing"
)

type BigQueryTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *Store
}

func (b *BigQueryTestSuite) SetupTest() {
	ctx := context.Background()
	b.fakeStore = &mocks.FakeStore{}
	store := db.Store(b.fakeStore)
	b.store = LoadBigQuery(ctx, &store)
}

func TestBigQueryTestSuite(t *testing.T) {
	suite.Run(t, new(BigQueryTestSuite))
}
