package redshift

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type RedshiftTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *Store
}

func (r *RedshiftTestSuite) SetupTest() {
	cfg := config.Config{
		Redshift: &config.Redshift{},
	}

	r.fakeStore = &mocks.FakeStore{}
	store := db.Store(r.fakeStore)
	r.store = LoadRedshift(cfg, &store)
	r.store.skipLgCols = true
}

func TestRedshiftTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftTestSuite))
}
