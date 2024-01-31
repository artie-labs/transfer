package types

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TypesTestSuite struct {
	suite.Suite
}

func TestTypesTestSuite(t *testing.T) {
	suite.Run(t, new(TypesTestSuite))
}
