package util

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type UtilTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (u *UtilTestSuite) SetupTest() {
	u.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
	})
}

func TestUtilTestSuite(t *testing.T) {
	suite.Run(t, new(UtilTestSuite))
}
