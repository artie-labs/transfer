package bigquery

import (
	"context"
	"github.com/artie-labs/transfer/clients/bigquery/clients"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
	"testing"
)

type BigQueryTestSuite struct {
	suite.Suite
	fakeClient *mocks.FakeClient
	store      *Store
}

func (b *BigQueryTestSuite) SetupTest() {
	ctx := context.Background()
	b.fakeClient = &mocks.FakeClient{}
	fakeClient := clients.Client(b.fakeClient)
	b.store = LoadBigQuery(ctx, fakeClient)
}

func TestBigQueryTestSuite(t *testing.T) {
	suite.Run(t, new(BigQueryTestSuite))
}
