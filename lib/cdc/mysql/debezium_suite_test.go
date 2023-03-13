package mysql

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type MySQLTestSuite struct {
	suite.Suite
	*Debezium
}

func (m *MySQLTestSuite) SetupTest() {
	var debezium Debezium
	m.Debezium = &debezium
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(MySQLTestSuite))
}
