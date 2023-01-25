package bigquery

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	_ "github.com/viant/bigquery"
	"os"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type BQStore struct {
	store db.Store
}

func (b *BQStore) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		return nil
	}

	return nil
}

func LoadBigQuery(ctx context.Context) *BQStore {
	if credPath := config.GetSettings().Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("error setting env var for %s", GooglePathToCredentialsEnvKey)
		}
	}

	bigqueryDSN := fmt.Sprintf("bigquery://%s/%s", config.GetSettings().Config.BigQuery.ProjectID,
		config.GetSettings().Config.BigQuery.Dataset)

	return &BQStore{
		store: db.Open(ctx, "bigquery", bigqueryDSN),
	}
}
