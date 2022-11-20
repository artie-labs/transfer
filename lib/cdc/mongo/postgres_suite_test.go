package mongo

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type MongoTestSuite struct {
	suite.Suite
	*Mongo
}

func (p *MongoTestSuite) SetupTest() {
	var debezium Mongo
	p.Mongo = &debezium
}

func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(MongoTestSuite))
}
