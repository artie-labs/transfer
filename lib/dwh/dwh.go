package dwh

import (
	"context"
	"github.com/artie-labs/transfer/clients/bigquery"
	"github.com/artie-labs/transfer/clients/bigquery/clients"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/optimization"
)

type DataWarehouse interface {
	Merge(ctx context.Context, tableData *optimization.TableData) error
	// TODO - we can further refactor the boilerplate-ness of Merge.
	// FQName() should be done at the DWH level.
}

func LoadDataWarehouse(ctx context.Context, store *db.Store, bqClient clients.Client) DataWarehouse {
	switch config.GetSettings().Config.Output {
	case "test":
		// TODO - In the future, we can create a fake store that follows the MERGE syntax for SQL standard.
		// Problem though, is that each DWH seems to implement MERGE differently.
		// So for now, the fake store will just output the merge command by following Snowflake's syntax.
		store := db.Store(&mock.DB{
			Fake: mocks.FakeStore{},
		})
		return snowflake.LoadSnowflake(ctx, &store)
	case "snowflake":
		return snowflake.LoadSnowflake(ctx, store)
	case "bigquery":
		return bigquery.LoadBigQuery(ctx, bqClient)
	}

	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"source": config.GetSettings().Config.Output,
	}).Fatal("No valid output sources specified.")

	return nil
}
