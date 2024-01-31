package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (m *MetricsTestSuite) SetupTest() {
	m.ctx = context.Background()
}

func TestMetricsTestSuite(t *testing.T) {
	suite.Run(t, new(MetricsTestSuite))
}
