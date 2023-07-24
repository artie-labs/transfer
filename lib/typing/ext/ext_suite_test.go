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
	e.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		Config: &config.Config{},
	})
}

func TestExtTestSuite(t *testing.T) {
	suite.Run(t, new(ExtTestSuite))
}
