package bigquery

import (
	"context"
	"fmt"
	"os"

	_ "github.com/viant/bigquery"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	configMap *types.DwhToTablesConfigMap
	db.Store
}

func LoadBigQuery(ctx context.Context, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	if credPath := config.GetSettings().Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		logger.FromContext(ctx).Debug("writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("error setting env var for %s", GooglePathToCredentialsEnvKey)
		}
	}

	return &Store{
		// TODO Allow specify data set
		Store: db.Open(ctx, "bigquery", fmt.Sprintf("bigquery://%s/customers_robin",
			config.GetSettings().Config.BigQuery.ProjectID)),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
