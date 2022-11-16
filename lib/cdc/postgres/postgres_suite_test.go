package postgres

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type PostgresTestSuite struct {
	suite.Suite
	*Debezium
}

func (p *PostgresTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}
