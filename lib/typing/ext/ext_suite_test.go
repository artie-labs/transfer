package ext

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ExtTestSuite struct {
	suite.Suite
}

func TestExtTestSuite(t *testing.T) {
	suite.Run(t, new(ExtTestSuite))
}
