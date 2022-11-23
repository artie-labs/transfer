package mongo

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type MongoTestSuite struct {
	suite.Suite
	*Debezium
}

func (p *MongoTestSuite) SetupTest() {
	var debezium Debezium
	p.Debezium = &debezium
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(MongoTestSuite))
}
