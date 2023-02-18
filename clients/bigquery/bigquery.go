package bigquery

import (
	"context"
	"fmt"
	"os"

	_ "github.com/viant/bigquery"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	configMap *types.DwhToTablesConfigMap
	db.Store
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.BigQuery
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
		Store: db.Open(ctx, "bigquery", fmt.Sprintf("bigquery://%s/%s",
			config.GetSettings().Config.BigQuery.ProjectID, config.GetSettings().Config.BigQuery.DefaultDataset)),
		configMap: &types.DwhToTablesConfigMap{},
	}
}
