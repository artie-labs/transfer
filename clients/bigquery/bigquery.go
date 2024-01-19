package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/lib/ptr"

	"cloud.google.com/go/bigquery"
	_ "github.com/viant/bigquery"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	describeNameCol               = "column_name"
	describeTypeCol               = "data_type"
	describeCommentCol            = "description"
)

type Store struct {
	configMap         *types.DwhToTablesConfigMap
	batchSize         int
	projectID         string
	uppercaseEscNames bool

	db.Store
}

func (s *Store) getTableConfig(ctx context.Context, tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return utils.GetTableConfig(ctx, utils.GetTableCfgArgs{
		Dwh:       s,
		FqName:    tableData.ToFqName(s.Label(), true, s.uppercaseEscNames, s.projectID),
		ConfigMap: s.configMap,
		Query: fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name='%s';",
			tableData.TopicConfig.Database, tableData.RawName()),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString(""),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
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

func (s *Store) GetClient(ctx context.Context) *bigquery.Client {
	settings := config.FromContext(ctx)
	client, err := bigquery.NewClient(ctx, settings.Config.BigQuery.ProjectID)
	if err != nil {
		logger.Fatal("failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) PutTable(ctx context.Context, dataset, tableName string, rows []*Row) error {
	client := s.GetClient(ctx)
	defer client.Close()

	batch := NewBatch(rows, s.batchSize)
	inserter := client.Dataset(dataset).Table(tableName).Inserter()
	for batch.HasNext() {
		if err := inserter.Put(ctx, batch.NextChunk()); err != nil {
			return fmt.Errorf("failed to insert rows, err: %v", err)
		}
	}

	return nil
}

func LoadBigQuery(ctx context.Context, _store *db.Store) *Store {
	settings := config.FromContext(ctx)
	settings.Config.BigQuery.LoadDefaultValues()
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			projectID:         settings.Config.BigQuery.ProjectID,
			uppercaseEscNames: settings.Config.SharedDestinationConfig.UppercaseEscapedNames,
			configMap:         &types.DwhToTablesConfigMap{},
		}
	}

	if credPath := settings.Config.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.Fatal(fmt.Sprintf("error setting env var for %s", GooglePathToCredentialsEnvKey), slog.Any("err", err))
		}
	}

	return &Store{
		Store: db.Open(ctx, "bigquery", settings.Config.BigQuery.DSN()),

		configMap:         &types.DwhToTablesConfigMap{},
		batchSize:         settings.Config.BigQuery.BatchSize,
		projectID:         settings.Config.BigQuery.ProjectID,
		uppercaseEscNames: settings.Config.SharedDestinationConfig.UppercaseEscapedNames,
	}
}
