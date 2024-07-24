package config

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	AccountID string `yaml:"account"`

	Username string `yaml:"username"`
	// If pathToPrivateKey is specified, the password field will be ignored
	PathToPrivateKey string `json:"pathToPrivateKey,omitempty"`
	Password         string `yaml:"password,omitempty"`

	Warehouse string `yaml:"warehouse"`
	Region    string `yaml:"region"`
	Host      string `yaml:"host"`

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
			// https://docs.snowflake.com/en/sql-reference/parameters#abort-detached-query
			"ABORT_DETACHED_QUERY": ptr.ToString("true"),
		},
	}

	if s.PathToPrivateKey != "" {
		key, err := loadPrivateKey(s.PathToPrivateKey)
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

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	return key, nil
}
