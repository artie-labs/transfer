package optimization

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type OptimizationTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (o *OptimizationTestSuite) SetupTest() {
	o.ctx = context.Background()
	o.ctx = config.InjectSettingsIntoContext(o.ctx, &config.Settings{Config: &config.Config{}})
}

func TestOptimizationTestSuite(t *testing.T) {
	suite.Run(t, new(OptimizationTestSuite))
}
