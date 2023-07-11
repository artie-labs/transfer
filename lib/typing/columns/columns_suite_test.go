package columns

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type ColumnsTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (c *ColumnsTestSuite) SetupTest() {
	c.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: false,
		Config:         &config.Config{},
	})
}

func TestColumnsTestSuite(t *testing.T) {
	suite.Run(t, new(ColumnsTestSuite))
}
