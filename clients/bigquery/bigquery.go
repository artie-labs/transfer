package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"cloud.google.com/go/bigquery"
	_ "github.com/viant/bigquery"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	describeNameCol               = "column_name"
	describeTypeCol               = "data_type"
	describeCommentCol            = "description"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	batchSize int
	config    config.Config

	db.Store
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings) error {
	// 1. Cast all the data into rows.
	var rows []*Row
	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := castColVal(value[col], colKind, additionalDateFmts)
			if err != nil {
				return fmt.Errorf("failed to cast col %s: %w", col, err)
			}

			if colVal != nil {
				data[col] = colVal
			}
		}

		rows = append(rows, NewRow(data))
	}

	// 2. Load the data into the temporary table.
	return s.putTable(context.Background(), tableData.TopicConfig.Database, tempTableName, rows)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return shared.GetTableConfig(shared.GetTableCfgArgs{
		Dwh:       s,
		FqName:    tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, s.config.BigQuery.ProjectID),
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
	client, err := bigquery.NewClient(ctx, s.config.BigQuery.ProjectID)
	if err != nil {
		logger.Panic("Failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) putTable(ctx context.Context, dataset, tableName string, rows []*Row) error {
	client := s.GetClient(ctx)
	defer client.Close()

	batch := NewBatch(rows, s.batchSize)
	inserter := client.Dataset(dataset).Table(tableName).Inserter()
	for batch.HasNext() {
		if err := inserter.Put(ctx, batch.NextChunk()); err != nil {
			return fmt.Errorf("failed to insert rows: %w", err)
		}
	}

	return nil
}

func LoadBigQuery(cfg config.Config, _store *db.Store) *Store {
	cfg.BigQuery.LoadDefaultValues()
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
		}
	}

	if credPath := cfg.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			logger.Panic(fmt.Sprintf("Error setting env var for %s", GooglePathToCredentialsEnvKey), slog.Any("err", err))
		}
	}

	return &Store{
		Store: db.Open("bigquery", cfg.BigQuery.DSN()),

		configMap: &types.DwhToTablesConfigMap{},
		batchSize: cfg.BigQuery.BatchSize,
		config:    cfg,
	}
}
