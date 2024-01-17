package mongo

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/logger"

	"github.com/stretchr/testify/suite"
)

type MongoTestSuite struct {
	suite.Suite
	*Debezium
	ctx context.Context
}

func (p *MongoTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium

	p.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		VerboseLogging: true,
		Config:         &config.Config{},
	})
	p.ctx = logger.InjectLoggerIntoCtx(p.ctx)
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(MongoTestSuite))
}
