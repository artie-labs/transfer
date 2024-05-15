package relational

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type RelationTestSuite struct {
	suite.Suite
	*Debezium
}

func (r *RelationTestSuite) SetupTest() {
	var debezium Debezium
	r.Debezium = &debezium
}

func TestRelationTestSuite(t *testing.T) {
	suite.Run(t, new(RelationTestSuite))
}
