package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestValidateMSSQL(t *testing.T) {
	var cfg Config
	assert.Error(t, cfg.ValidateMSSQL())
	cfg.Output = constants.MSSQL
	assert.Error(t, cfg.ValidateMSSQL())
	cfg.MSSQL = &MSSQL{}
	assert.Error(t, cfg.ValidateMSSQL())

	cfg.MSSQL = &MSSQL{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "password",
		Database: "test",
	}
	assert.NoError(t, cfg.ValidateMSSQL())
}
