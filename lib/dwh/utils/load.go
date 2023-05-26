package utils

import (
	"context"

	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/mocks"
)

func DataWarehouse(ctx context.Context, store *db.Store) dwh.DataWarehouse {
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
		return snowflake.LoadSnowflake(ctx, &store, false)
	case constants.Snowflake:
		return snowflake.LoadSnowflake(ctx, store, false)
	case constants.SnowflakeStages:
		s := snowflake.LoadSnowflake(ctx, store, true)
		if err := s.Sweep(ctx); err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("failed to clean up snowflake")
		}

		return s
	case constants.BigQuery:
		return bigquery.LoadBigQuery(ctx, store)
	}

	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"source": settings.Config.Output,
	}).Fatal("No valid output sources specified.")

	return nil
}
