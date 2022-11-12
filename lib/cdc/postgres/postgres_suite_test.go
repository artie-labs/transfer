package postgres

import "github.com/stretchr/testify/suite"

type PostgresTestSuite struct {
	suite.Suite
	*Debezium
}

func (p *PostgresTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium
}
