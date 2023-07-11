package sql

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type SqlTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (s *SqlTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.ctx = config.InjectSettingsIntoContext(s.ctx, &config.Settings{Config: &config.Config{}})
}

func TestSqlTestSuite(t *testing.T) {
	suite.Run(t, new(SqlTestSuite))
}
