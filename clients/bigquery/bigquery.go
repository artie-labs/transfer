package bigquery

import (
	"context"
	"github.com/artie-labs/transfer/clients/bigquery/clients"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"os"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	configMap *types.DwhToTablesConfigMap
	clients.Client
}

func LoadBigQuery(ctx context.Context, _client clients.Client) *Store {
	if _client != nil {
		// Used for tests.
		return &Store{
			Client:    _client,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	if credPath := config.GetSettings().Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatalf("error setting env var for %s", GooglePathToCredentialsEnvKey)
		}
	}

	return &Store{
		Client:    clients.NewClient(ctx, config.GetSettings().Config.BigQuery.ProjectID),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
