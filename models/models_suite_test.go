package models

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ModelsTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	ctx       context.Context
}

func (m *ModelsTestSuite) SetupTest() {
	LoadMemoryDB()
	m.ctx = context.Background()
	m.ctx = config.InjectSettingsIntoContext(m.ctx, &config.Settings{
		Config: &config.Config{
			FlushIntervalSeconds: 10,
			BufferRows:           10,
		},
	})
}

func TestModelsTestSuite(t *testing.T) {
	suite.Run(t, new(ModelsTestSuite))
}
