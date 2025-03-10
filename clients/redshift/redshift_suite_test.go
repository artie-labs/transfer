package redshift

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
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
	var err error
	r.store, err = LoadRedshift(r.T().Context(), cfg, &store)
	assert.NoError(r.T(), err)
}

func TestRedshiftTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftTestSuite))
}
