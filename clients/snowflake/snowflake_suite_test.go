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
	fakeStageStore *mocks.FakeStore
	stageStore     *Store
	ctx            context.Context
}

func (s *SnowflakeTestSuite) SetupTest() {
	s.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})

	s.fakeStageStore = &mocks.FakeStore{}
	stageStore := db.Store(s.fakeStageStore)
	s.stageStore = LoadSnowflake(s.ctx, &stageStore)
}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
