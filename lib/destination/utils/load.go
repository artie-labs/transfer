package utils

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/databricks"
	"github.com/artie-labs/transfer/clients/iceberg"
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
	return cfg.Output == constants.S3 || cfg.Output == constants.Iceberg
}

func LoadBaseline(ctx context.Context, cfg config.Config) (destination.Baseline, error) {
	switch cfg.Output {
	case constants.S3:
		store, err := s3.LoadStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load S3: %w", err)
		}

		return store, nil
	case constants.Iceberg:
		store, err := iceberg.LoadStore(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load Iceberg: %w", err)
		}
		return store, nil
	}

	return nil, fmt.Errorf("invalid baseline output source specified: %q", cfg.Output)
}

func LoadDestination(ctx context.Context, cfg config.Config, store *db.Store) (destination.Destination, error) {
	switch cfg.Output {
	case constants.Snowflake:
		return snowflake.LoadSnowflake(ctx, cfg, store)
	case constants.BigQuery:
		return bigquery.LoadBigQuery(ctx, cfg, store)
	case constants.Databricks:
		return databricks.LoadStore(cfg)
	case constants.MSSQL:
		return mssql.LoadStore(cfg)
	case constants.Redshift:
		return redshift.LoadRedshift(ctx, cfg, store)
	}

	return nil, fmt.Errorf("invalid destination: %q", cfg.Output)
}
