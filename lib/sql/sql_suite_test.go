package sql

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type SqlTestSuite struct {
	suite.Suite
}

func TestSqlTestSuite(t *testing.T) {
	suite.Run(t, new(SqlTestSuite))
}
