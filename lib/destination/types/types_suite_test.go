package types

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type TypesTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (t *TypesTestSuite) SetupTest() {
	t.ctx = context.Background()
	t.ctx = config.InjectSettingsIntoContext(t.ctx, &config.Settings{Config: &config.Config{}})
}

func TestTypesTestSuite(t *testing.T) {
	suite.Run(t, new(TypesTestSuite))
}
