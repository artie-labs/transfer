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
	"github.com/artie-labs/transfer/lib/destination"
)

// Load returns a [destination.Destination] for any supported output.
// For SQL destinations, the returned value also implements [destination.SQLDestination].
func Load(ctx context.Context, cfg config.Config) (destination.Destination, error) {
	switch cfg.Output {
	// SQL destinations
	case constants.Snowflake:
		return snowflake.LoadStore(ctx, cfg, nil)
	case constants.BigQuery:
		return bigquery.LoadStore(ctx, cfg, nil)
	case constants.Databricks:
		return databricks.LoadStore(cfg)
	case constants.MSSQL:
		return mssql.LoadStore(cfg)
	case constants.MySQL:
		return mysql.LoadStore(cfg)
	case constants.Postgres:
		return postgres.LoadStore(ctx, cfg)
	case constants.Redshift:
		return redshift.LoadStore(ctx, cfg, nil)
	case constants.MotherDuck:
		return motherduck.LoadStore(cfg)
	case constants.Clickhouse:
		return clickhouse.LoadStore(ctx, cfg, nil)

	// Object storage destinations
	case constants.S3:
		return s3.LoadStore(ctx, cfg)
	case constants.GCS:
		return gcs.LoadStore(ctx, cfg)
	case constants.Iceberg:
		store, err := iceberg.LoadStore(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return store, nil

	// Streaming destinations
	case constants.Redis:
		return redis.LoadStore(ctx, cfg)
	case constants.SQS:
		return sqs.LoadStore(ctx, cfg)
	}

	return nil, fmt.Errorf("invalid destination: %q", cfg.Output)
}

// LoadSQLDestination returns a [destination.SQLDestination] for SQL-based outputs only.
// This is a convenience wrapper for callers that specifically need a SQL destination (e.g., integration tests).
func LoadSQLDestination(ctx context.Context, cfg config.Config) (destination.SQLDestination, error) {
	dest, err := Load(ctx, cfg)
	if err != nil {
		return nil, err
	}

	sqlDest, ok := dest.(destination.SQLDestination)
	if !ok {
		return nil, fmt.Errorf("destination %q is not a SQL destination", cfg.Output)
	}

	return sqlDest, nil
}
