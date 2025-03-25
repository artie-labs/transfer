package config

import (
	"cmp"
	"fmt"
	"net/url"
	"strings"

	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/snowflakedb/gosnowflake"
)

// DSN - returns the notation for BigQuery following this format: bigquery://projectID/[location/]datasetID?queryString
// If location is passed in, we'll specify it. Else, it'll default to empty and our library will set it to US.
func (b *BigQuery) DSN() string {
	dsn := fmt.Sprintf("bigquery://%s/%s", b.ProjectID, b.DefaultDataset)

	if b.Location != "" {
		dsn = fmt.Sprintf("bigquery://%s/%s/%s", b.ProjectID, b.Location, b.DefaultDataset)
	}

	return dsn
}

func (m MSSQL) DSN() string {
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

func (s Snowflake) ToConfig() (*gosnowflake.Config, error) {
	cfg := &gosnowflake.Config{
		OCSPFailOpen: gosnowflake.OCSPFailOpenTrue,
		Account:      s.AccountID,
		User:         s.Username,
		Warehouse:    s.Warehouse,
		Region:       s.Region,
		Application:  s.Application,
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

func (d Databricks) DSN() string {
	query := url.Values{}
	query.Add("catalog", d.Catalog)
	u := &url.URL{
		Path:     d.HttpPath,
		User:     url.UserPassword("token", d.PersonalAccessToken),
		Host:     fmt.Sprintf("%s:%d", d.Host, cmp.Or(d.Port, 443)),
		RawQuery: query.Encode(),
	}

	return strings.TrimPrefix(u.String(), "//")
}
