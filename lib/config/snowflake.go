package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/crypto"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	AccountID string `yaml:"account"`
	Username  string `yaml:"username"`
	// If pathToPrivateKey is specified, the password field will be ignored
	PathToPrivateKey string `yaml:"pathToPrivateKey,omitempty"`
	Password         string `yaml:"password,omitempty"`

	Warehouse   string `yaml:"warehouse"`
	Region      string `yaml:"region"`
	Host        string `yaml:"host"`
	Application string `yaml:"application"`
}

func (s Snowflake) ToConfig() (*gosnowflake.Config, error) {
	cfg := &gosnowflake.Config{
		Account:     s.AccountID,
		User:        s.Username,
		Warehouse:   s.Warehouse,
		Region:      s.Region,
		Application: s.Application,
		Params: map[string]*string{
			// This parameter will cancel in-progress queries if connectivity is lost.
			// https://docs.snowflake.com/en/sql-reference/parameters#abort-detached-query
			"ABORT_DETACHED_QUERY": ptr.ToString("true"),
			// This parameter must be set to prevent the auth token from expiring after 4 hours.
			// https://docs.snowflake.com/en/user-guide/session-policies#considerations
			"CLIENT_SESSION_KEEP_ALIVE": ptr.ToString("true"),
		},
	}

	if s.PathToPrivateKey != "" {
		key, err := crypto.LoadRSAKey(s.PathToPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load private key: %w", err)
		}

		cfg.PrivateKey = key
		cfg.Authenticator = gosnowflake.AuthTypeJwt
	} else {
		cfg.Password = s.Password
	}

	if s.Host != "" {
		// If the host is specified
		cfg.Host = s.Host
		cfg.Region = ""
	}

	return cfg, nil
}
