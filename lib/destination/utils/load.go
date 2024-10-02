package utils

import (
	"fmt"

	"github.com/artie-labs/transfer/clients/databricks"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/mssql"
	"github.com/artie-labs/transfer/clients/redshift"
	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
)

func IsOutputBaseline(cfg config.Config) bool {
	return cfg.Output == constants.S3
}

func LoadBaseline(cfg config.Config) (destination.Baseline, error) {
	switch cfg.Output {
	case constants.S3:
		store, err := s3.LoadStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load S3: %w", err)
		}

		return store, nil
	}

	return nil, fmt.Errorf("invalid baseline output source specified: %q", cfg.Output)
}

func LoadDataWarehouse(cfg config.Config, store *db.Store) (destination.DataWarehouse, error) {
	switch cfg.Output {
	case constants.Snowflake:
		s, err := snowflake.LoadSnowflake(cfg, store)
		if err != nil {
			return nil, err
		}
		if err = s.Sweep(); err != nil {
			return nil, fmt.Errorf("failed to clean up Snowflake: %w", err)
		}
		return s, nil
	case constants.BigQuery:
		return bigquery.LoadBigQuery(cfg, store)
	case constants.Databricks:
		return databricks.LoadStore(cfg)
	case constants.MSSQL:
		s, err := mssql.LoadStore(cfg)
		if err != nil {
			return nil, err
		}
		if err = s.Sweep(); err != nil {
			return nil, fmt.Errorf("failed to clean up MS SQL: %w", err)
		}
		return s, nil
	case constants.Redshift:
		s, err := redshift.LoadRedshift(cfg, store)
		if err != nil {
			return nil, err
		}
		if err = s.Sweep(); err != nil {
			return nil, fmt.Errorf("failed to clean up Redshift: %w", err)
		}
		return s, nil
	}

	return nil, fmt.Errorf("invalid data warehouse output source specified: %q", cfg.Output)
}
