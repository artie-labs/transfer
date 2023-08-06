package parquetutil

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/stretchr/testify/suite"
)

type ParquetUtilTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (p *ParquetUtilTestSuite) SetupTest() {
	p.ctx = config.InjectSettingsIntoContext(context.Background(), &config.Settings{
		Config: &config.Config{},
	})
}

func TestParquetUtilTestSuite(t *testing.T) {
	suite.Run(t, new(ParquetUtilTestSuite))
}
