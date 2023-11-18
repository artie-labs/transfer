package redshift

import (
	"context"
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
	ctx       context.Context
}

func (r *RedshiftTestSuite) SetupTest() {
	r.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
		Config: &config.Config{
			Redshift: &config.Redshift{},
		},
	})

	r.fakeStore = &mocks.FakeStore{}
	store := db.Store(r.fakeStore)
	r.store = LoadRedshift(r.ctx, &store)
	r.store.skipLgCols = true
}

func TestRedshiftTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftTestSuite))
}
