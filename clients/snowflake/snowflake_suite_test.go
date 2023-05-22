package snowflake

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
)

type SnowflakeTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	store     *Store
	ctx       context.Context
}

func (s *SnowflakeTestSuite) SetupTest() {
	s.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})

	s.fakeStore = &mocks.FakeStore{}
	store := db.Store(s.fakeStore)
	s.store = LoadSnowflake(s.ctx, &store, false)

}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
