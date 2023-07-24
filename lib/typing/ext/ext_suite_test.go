package ext

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type ExtTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (e *ExtTestSuite) SetupTest() {
	e.ctx = context.Background()
	e.ctx = config.InjectSettingsIntoContext(e.ctx, &config.Settings{
		Config: &config.Config{
			FlushIntervalSeconds: 10,
			BufferRows:           10,
		},
	})
}

func TestExtTestSuite(t *testing.T) {
	suite.Run(t, new(ExtTestSuite))
}
