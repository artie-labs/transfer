package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/mocks"
)

type SnowflakeTestSuite struct {
	suite.Suite
	fakeStageStore *mocks.FakeStore
	stageStore     *Store
}

func (s *SnowflakeTestSuite) SetupTest() {
	s.ResetStore()
}

func (s *SnowflakeTestSuite) ResetStore() {
	s.fakeStageStore = &mocks.FakeStore{}
	stageStore := db.Store(s.fakeStageStore)
	var err error
	s.stageStore, err = LoadSnowflake(config.Config{
		Snowflake: &config.Snowflake{},
	}, &stageStore)
	assert.NoError(s.T(), err)
}

func TestSnowflakeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeTestSuite))
}
