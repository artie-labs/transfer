package config

import (
	"cmp"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/typing"
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

func (p Postgres) DSN() string {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.Username, p.Password, p.Host, p.Port, p.Database)
	if p.DisableSSL {
		dsn = fmt.Sprintf("%s?sslmode=disable", dsn)
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

func (m MySQL) DSN() string {
	config := mysql.NewConfig()
	config.User = m.Username
	config.Passwd = m.Password
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", m.Host, m.Port)
	config.DBName = m.Database
	config.ParseTime = true
	// If we don't specify this, it will default to the server's timezone.
	// This will then cause a bug because MySQL is internally storing the TIMESTAMP value in UTC format.
	// We use '+00:00' instead of 'UTC' because some MySQL servers don't have timezone tables populated,
	// which would cause: Error 1298 (HY000): Unknown or incorrect time zone: 'UTC'
	config.Params = map[string]string{
		"time_zone": "'+00:00'",
	}
	return config.FormatDSN()
}

func (s Snowflake) ToConfig() (*gosnowflake.Config, error) {
	cfg := &gosnowflake.Config{
		Account:     s.AccountID,
		User:        s.Username,
		Warehouse:   s.Warehouse,
		Role:        s.Role,
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

	for key, value := range s.AdditionalParameters {
		cfg.Params[key] = &value
		slog.Info("Setting additional parameters for Snowflake", slog.String("key", key), slog.String("value", value))
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
