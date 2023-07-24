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

func (t *TypingTestSuite) SetUpTest() {
	t.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})
}

func TestTypingTestSuite(t *testing.T) {
	suite.Run(t, new(TypingTestSuite))
}
