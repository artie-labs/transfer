package metrics

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (m *MetricsTestSuite) SetupTest() {
	m.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		Config:         &config.Config{},
		VerboseLogging: false,
	})
}

func TestMetricsTestSuite(t *testing.T) {
	suite.Run(t, new(MetricsTestSuite))
}
