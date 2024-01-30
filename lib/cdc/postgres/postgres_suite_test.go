package postgres

import (
	"testing"

	"github.com/stretchr/testify/suite"
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
