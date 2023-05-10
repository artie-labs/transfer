package models

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/suite"
)

type ModelsTestSuite struct {
	suite.Suite
	fakeStore *mocks.FakeStore
	ctx       context.Context
}

func (m *ModelsTestSuite) SetupTest() {

	m.ctx = context.Background()
	m.ctx = config.InjectSettingsIntoContext(m.ctx, &config.Settings{
		Config: &config.Config{
			FlushIntervalSeconds: 10,
			BufferRows:           10,
		},
	})

	m.ctx = LoadMemoryDB(m.ctx)
}

func TestModelsTestSuite(t *testing.T) {
	suite.Run(t, new(ModelsTestSuite))
}
