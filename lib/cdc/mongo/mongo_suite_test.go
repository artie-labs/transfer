package mongo

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MongoTestSuite struct {
	suite.Suite
	*Debezium
}

func (m *MongoTestSuite) SetupTest() {
	var debezium Debezium
	m.Debezium = &debezium
}

func TestMongoTestSuite(t *testing.T) {
	suite.Run(t, new(MongoTestSuite))
}
