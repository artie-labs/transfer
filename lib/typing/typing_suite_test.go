package typing

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TypingTestSuite struct {
	suite.Suite
}

func TestTypingTestSuite(t *testing.T) {
	suite.Run(t, new(TypingTestSuite))
}
