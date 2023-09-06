package postgres

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/stretchr/testify/suite"
)

type PostgresTestSuite struct {
	suite.Suite
	*Debezium
	ctx context.Context
}

func (p *PostgresTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium
	p.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
	})
	p.ctx = logger.InjectLoggerIntoCtx(p.ctx)
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}
