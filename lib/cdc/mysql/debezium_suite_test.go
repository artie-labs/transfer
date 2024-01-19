package mysql

import (
	"testing"

	"github.com/stretchr/testify/suite"
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
