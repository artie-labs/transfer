package dwh

import (
	"context"
	"github.com/artie-labs/transfer/clients/bigquery"
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
}

func LoadDataWarehouse(ctx context.Context) DataWarehouse {
	switch config.GetSettings().Config.Output {
	// TODO "test", "snowflake", "bigquery" should all be valid labels and variables.
	case "test":
		store := db.Store(&mock.DB{
			Fake: mocks.FakeStore{},
		})
		// TODO: When we create mock DWH interfaces, instantiate a mock DWH store
		return snowflake.LoadSnowflake(ctx, &store)
	case "snowflake":
		return snowflake.LoadSnowflake(ctx, nil)
	case "bigquery":
		return bigquery.LoadBigQuery(ctx)
	}

	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"source": config.GetSettings().Config.Output,
	}).Fatal("No valid output sources specified.")

	return nil
}
