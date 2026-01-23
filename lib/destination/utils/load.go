package utils

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/clickhouse"
	"github.com/artie-labs/transfer/clients/databricks"
	"github.com/artie-labs/transfer/clients/gcs"
	"github.com/artie-labs/transfer/clients/iceberg"
	"github.com/artie-labs/transfer/clients/motherduck"
	"github.com/artie-labs/transfer/clients/mssql"
	"github.com/artie-labs/transfer/clients/mysql"
	"github.com/artie-labs/transfer/clients/postgres"
	"github.com/artie-labs/transfer/clients/redis"
	"github.com/artie-labs/transfer/clients/redshift"
	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/clients/sqs"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
)

// IsOutputBaseline returns true if the output destination only implements the Baseline interface
// (e.g., blob/object storage like S3, GCS, or specialized destinations like Iceberg, Redis).
// These destinations implement Merge and Append directly without using shared SQL-based logic.
func IsOutputBaseline(cfg config.Config) bool {
	switch cfg.Output {
	case constants.S3, constants.GCS, constants.Iceberg, constants.Redis, constants.SQS:
		return true
	default:
		return false
	}
}

// LoadBaseline loads destinations that only implement the Baseline interface.
// These are typically blob/object storage destinations (S3, GCS) or specialized
// destinations like Iceberg and Redis that implement Merge and Append directly.
func LoadBaseline(ctx context.Context, cfg config.Config) (destination.Baseline, error) {
	switch cfg.Output {
	case constants.S3:
		store, err := s3.LoadStore(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load S3: %w", err)
		}

		return store, nil
	case constants.GCS:
		store, err := gcs.LoadStore(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load GCS: %w", err)
		}

		return store, nil
	case constants.Iceberg:
		store, err := iceberg.LoadStore(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load Iceberg: %w", err)
		}
		return store, nil
	case constants.Redis:
		store, err := redis.LoadRedis(ctx, cfg, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to load Redis: %w", err)
		}
		return store, nil
	case constants.SQS:
		store, err := sqs.LoadSQS(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load SQS: %w", err)
		}
		return store, nil
	}

	return nil, fmt.Errorf("invalid baseline output source specified: %q", cfg.Output)
}

// LoadDestination loads SQL-based destinations that implement the full Destination interface.
// These destinations use SQL for data operations and implement additional interfaces like
// SQLExecutor, DialectAware, TemporaryTableManager, Deduplicator, and DataLoader.
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
	case constants.MySQL:
		return mysql.LoadStore(cfg)
	case constants.Postgres:
		return postgres.LoadStore(ctx, cfg)
	case constants.Redshift:
		return redshift.LoadRedshift(ctx, cfg, store)
	case constants.MotherDuck:
		return motherduck.LoadStore(cfg)
	case constants.Clickhouse:
		return clickhouse.LoadClickhouse(ctx, cfg, store)
	}

	return nil, fmt.Errorf("invalid destination: %q", cfg.Output)
}
