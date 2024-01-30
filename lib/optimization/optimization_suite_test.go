package optimization

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type OptimizationTestSuite struct {
	suite.Suite
}

func TestOptimizationTestSuite(t *testing.T) {
	suite.Run(t, new(OptimizationTestSuite))
}
