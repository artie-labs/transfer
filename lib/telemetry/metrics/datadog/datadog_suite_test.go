package datadog

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DatadogTestSuite struct {
	suite.Suite
}

func TestDatadogTestSuite(t *testing.T) {
	suite.Run(t, new(DatadogTestSuite))
}
