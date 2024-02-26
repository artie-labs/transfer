package config

import (
	"fmt"
	"net/url"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type MSSQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (m *MSSQL) DSN() string {
	query := url.Values{}
	query.Add("database", m.Database)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(m.Username, m.Password),
		Host:     fmt.Sprintf("%s:%d", m.Host, m.Port),
		RawQuery: query.Encode(),
	}

	return u.String()
}

func (c Config) ValidateMSSQL() error {
	if c.Output != constants.MSSQL {
		return fmt.Errorf("output is not mssql, output: %v", c.Output)
	}

	if c.MSSQL == nil {
		return fmt.Errorf("mssql config is nil")
	}

	if empty := stringutil.Empty(c.MSSQL.Host, c.MSSQL.Username, c.MSSQL.Password, c.MSSQL.Database); empty {
		return fmt.Errorf("one of mssql settings is empty (host, username, password, database)")
	}

	if c.MSSQL.Port <= 0 {
		return fmt.Errorf("invalid mssql port: %d", c.MSSQL.Port)
	}

	return nil
}
