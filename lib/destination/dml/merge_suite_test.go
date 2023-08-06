package dml

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type MergeTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (m *MergeTestSuite) SetupTest() {
	m.ctx = context.Background()
	m.ctx = config.InjectSettingsIntoContext(m.ctx, &config.Settings{Config: &config.Config{}})
}

func TestMergeTestSuite(t *testing.T) {
	suite.Run(t, new(MergeTestSuite))
}
