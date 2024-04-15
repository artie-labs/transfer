package utils

import (
	"fmt"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/mssql"
	"github.com/artie-labs/transfer/clients/redshift"
	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/mocks"
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
	case "test":
		// TODO - In the future, we can create a fake store that follows the MERGE syntax for SQL standard.
		// Also, the fake library not only needs to support MERGE, but needs to be able to make it easy for us to return
		// The results via results.Sql (from the database/sql library)
		// Problem though, is that each DWH seems to implement MERGE differently.
		// So for now, the fake store will just output the merge command by following Snowflake's syntax.
		store := db.Store(&mock.DB{
			Fake: mocks.FakeStore{},
		})
		return snowflake.LoadSnowflake(cfg, &store)
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
