package parquetutil

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ParquetUtilTestSuite struct {
	suite.Suite
}

func TestParquetUtilTestSuite(t *testing.T) {
	suite.Run(t, new(ParquetUtilTestSuite))
}
