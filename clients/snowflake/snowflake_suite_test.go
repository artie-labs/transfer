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
	fakeStore      *mocks.FakeStore
	fakeStageStore *mocks.FakeStore
	store          *Store
	stageStore     *Store
	ctx            context.Context
}

func (s *SnowflakeTestSuite) SetupTest() {
	s.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})

	s.fakeStore = &mocks.FakeStore{}
	store := db.Store(s.fakeStore)
	s.store = LoadSnowflake(s.ctx, &store, false)

	s.fakeStageStore = &mocks.FakeStore{}
	stageStore := db.Store(s.fakeStageStore)
	s.stageStore = LoadSnowflake(s.ctx, &stageStore, true)

}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
