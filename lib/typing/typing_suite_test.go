package typing

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type TypingTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (t *TypingTestSuite) SetupTest() {
	t.ctx = context.Background() // Initialize the context
	t.ctx = config.InjectSettingsIntoContext(t.ctx, &config.Settings{
		VerboseLogging: false,
		Config:         &config.Config{},
	})
}

func TestTypingTestSuite(t *testing.T) {
	suite.Run(t, new(TypingTestSuite))
}
