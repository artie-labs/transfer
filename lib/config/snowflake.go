package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/snowflakedb/gosnowflake"
)

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
			"ABORT_DETACHED_QUERY": typing.ToPtr("true"),
			// This parameter must be set to prevent the auth token from expiring after 4 hours.
			// https://docs.snowflake.com/en/user-guide/session-policies#considerations
			"CLIENT_SESSION_KEEP_ALIVE": typing.ToPtr("true"),
		},
	}

	if s.PathToPrivateKey != "" {
		key, err := cryptography.LoadRSAKey(s.PathToPrivateKey)
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
