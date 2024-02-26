package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestValidateMSSQL(t *testing.T) {
	var cfg Config
	assert.ErrorContains(t, cfg.ValidateMSSQL(), "output is not mssql")
	cfg.Output = constants.MSSQL
	assert.ErrorContains(t, cfg.ValidateMSSQL(), "mssql config is nil")
	cfg.MSSQL = &MSSQL{}
	assert.ErrorContains(t, cfg.ValidateMSSQL(), "one of mssql settings is empty (host, username, password, database)")

	cfg.MSSQL = &MSSQL{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "password",
		Database: "test",
	}
	assert.NoError(t, cfg.ValidateMSSQL())
}
