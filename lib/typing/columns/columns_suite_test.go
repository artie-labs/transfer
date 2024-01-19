package columns

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ColumnsTestSuite struct {
	suite.Suite
}

func TestColumnsTestSuite(t *testing.T) {
	suite.Run(t, new(ColumnsTestSuite))
}
