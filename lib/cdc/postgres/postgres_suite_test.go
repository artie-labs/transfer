package postgres

import (
	"context"
	"github.com/stretchr/testify/suite"
	"testing"
)

type PostgresTestSuite struct {
	suite.Suite
	*Debezium
	ctx context.Context
}

func (p *PostgresTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium
	p.ctx = context.Background()
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}
