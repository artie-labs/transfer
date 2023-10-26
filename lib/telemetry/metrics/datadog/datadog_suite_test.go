package datadog

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/stretchr/testify/suite"
)

type DatadogTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (d *DatadogTestSuite) SetupTest() {
	d.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		Config:         &config.Config{},
		VerboseLogging: false,
	})
}

func TestDatadogTestSuite(t *testing.T) {
	suite.Run(t, new(DatadogTestSuite))
}
