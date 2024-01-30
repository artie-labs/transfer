package dml

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MergeTestSuite struct {
	suite.Suite
}

func TestMergeTestSuite(t *testing.T) {
	suite.Run(t, new(MergeTestSuite))
}
