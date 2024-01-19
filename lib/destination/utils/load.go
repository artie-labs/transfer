package utils

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/redshift"
	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/mocks"
)

func IsOutputBaseline(ctx context.Context) bool {
	return config.FromContext(ctx).Config.Output == constants.S3
}

func Baseline(ctx context.Context) destination.Baseline {
	settings := config.FromContext(ctx)
	switch settings.Config.Output {
	case constants.S3:
		store, err := s3.LoadStore(settings.Config.S3)
		if err != nil {
			logger.Fatal("failed to load s3", slog.Any("err", err))
		}

		return store
	}

	logger.Fatal("No valid output sources specified", slog.Any("source", settings.Config.Output))

	return nil
}

func DataWarehouse(ctx context.Context, store *db.Store) destination.DataWarehouse {
	settings := config.FromContext(ctx)

	switch settings.Config.Output {
	case "test":
		// TODO - In the future, we can create a fake store that follows the MERGE syntax for SQL standard.
		// Also, the fake library not only needs to support MERGE, but needs to be able to make it easy for us to return
		// The results via results.Sql (from the database/sql library)
		// Problem though, is that each DWH seems to implement MERGE differently.
		// So for now, the fake store will just output the merge command by following Snowflake's syntax.
		store := db.Store(&mock.DB{
			Fake: mocks.FakeStore{},
		})
		return snowflake.LoadSnowflake(ctx, &store)
	case constants.Snowflake:
		s := snowflake.LoadSnowflake(ctx, store)
		if err := s.Sweep(ctx); err != nil {
			logger.Fatal("failed to clean up snowflake", slog.Any("err", err))
		}
		return s
	case constants.BigQuery:
		return bigquery.LoadBigQuery(ctx, store)
	case constants.Redshift:
		s := redshift.LoadRedshift(ctx, store)
		if err := s.Sweep(ctx); err != nil {
			logger.Fatal("failed to clean up redshift", slog.Any("err", err))
		}
		return s
	}

	logger.Fatal("No valid output sources specified.", slog.Any("source", settings.Config.Output))

	return nil
}
