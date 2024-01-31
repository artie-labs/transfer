package utils

import (
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

func IsOutputBaseline(cfg config.Config) bool {
	return cfg.Output == constants.S3
}

func Baseline(cfg config.Config) destination.Baseline {
	switch cfg.Output {
	case constants.S3:
		store, err := s3.LoadStore(cfg)
		if err != nil {
			logger.Panic("Failed to load s3", slog.Any("err", err))
		}

		return store
	}

	logger.Panic("No valid output sources specified", slog.Any("source", cfg.Output))

	return nil
}

func DataWarehouse(cfg config.Config, store *db.Store) destination.DataWarehouse {
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
		s := snowflake.LoadSnowflake(cfg, store)
		if err := s.Sweep(); err != nil {
			logger.Panic("failed to clean up snowflake", slog.Any("err", err))
		}
		return s
	case constants.BigQuery:
		return bigquery.LoadBigQuery(cfg, store)
	case constants.Redshift:
		s := redshift.LoadRedshift(cfg, store)
		if err := s.Sweep(); err != nil {
			logger.Panic("failed to clean up redshift", slog.Any("err", err))
		}
		return s
	}

	logger.Panic("No valid output sources specified", slog.Any("source", cfg.Output))

	return nil
}
