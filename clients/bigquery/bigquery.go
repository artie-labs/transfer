package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	_ "github.com/viant/bigquery"
	"google.golang.org/protobuf/proto"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/batch"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	describeNameCol               = "column_name"
	describeTypeCol               = "data_type"
	describeCommentCol            = "description"
	// Storage Write API is limited to 10 MiB, subtract 50 KiB to account for request overhead.
	maxRequestByteSize = (10 * 1024 * 1024) - (50 * 1024)
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	config    config.Config

	db.Store
}

func (s *Store) Append(tableData *optimization.TableData, useTempTable bool) error {
	if !useTempTable {
		return shared.Append(s, tableData, types.AdditionalSettings{})
	}

	// We can simplify this once Google has fully rolled out the ability to execute DML on recently streamed data
	// See: https://cloud.google.com/bigquery/docs/write-api#use_data_manipulation_language_dml_with_recently_streamed_data
	// For now, we'll need to append this to a temporary table and then append temporary table onto the target table
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	temporaryTableID := shared.TempTableID(tableID)

	defer func() { _ = ddl.DropTemporaryTable(s, temporaryTableID, false) }()

	err := shared.Append(s, tableData, types.AdditionalSettings{
		UseTempTable: true,
		TempTableID:  temporaryTableID,
	})

	if err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s`,
		tableID.FullyQualifiedName(),
		strings.Join(sql.QuoteIdentifiers(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(), s.Dialect()), ","),
		strings.Join(sql.QuoteIdentifiers(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(), s.Dialect()), ","),
		temporaryTableID.FullyQualifiedName(),
	)

	if _, err = s.Exec(query); err != nil {
		return fmt.Errorf("failed to insert data into target table: %w", err)
	}

	return nil
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dialect:        s.Dialect(),
			Tc:             tableConfig,
			TableID:        tempTableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			Mode:           tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(s, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	bqTempTableID, ok := tempTableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("unable to cast tempTableID to BigQuery TableIdentifier")
	}

	return s.putTable(context.Background(), bqTempTableID, tableData)
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(s.config.BigQuery.ProjectID, topicConfig.Database, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", tableData.TopicConfig().Database)
	return shared.GetTableCfgArgs{
		Dwh:                s,
		TableID:            s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               []any{tableData.Name()},
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString(""),
		DropDeletedColumns: tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return dialect.BigQueryDialect{}
}

func (s *Store) AdditionalDateFormats() []string {
	return s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
}

func (s *Store) GetClient(ctx context.Context) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, s.config.BigQuery.ProjectID)
	if err != nil {
		logger.Panic("Failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) putTable(ctx context.Context, bqTableID TableIdentifier, tableData *optimization.TableData) error {
	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()

	messageDescriptor, err := columnsToMessageDescriptor(columns)
	if err != nil {
		return err
	}
	schemaDescriptor, err := adapt.NormalizeDescriptor(*messageDescriptor)
	if err != nil {
		return err
	}

	managedWriterClient, err := managedwriter.NewClient(ctx, bqTableID.ProjectID())
	if err != nil {
		return fmt.Errorf("failed to create managedwriter client: %w", err)
	}
	defer managedWriterClient.Close()

	managedStream, err := managedWriterClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(
			managedwriter.TableParentFromParts(bqTableID.ProjectID(), bqTableID.Dataset(), bqTableID.Table()),
		),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(schemaDescriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return fmt.Errorf("failed to create managed stream: %w", err)
	}
	defer managedStream.Close()

	encoder := func(row map[string]any) ([]byte, error) {
		message, err := rowToMessage(row, columns, *messageDescriptor, s.AdditionalDateFormats())
		if err != nil {
			return nil, fmt.Errorf("failed to convert row to message: %w", err)
		}

		bytes, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}

		return bytes, nil
	}

	return batch.BySize(tableData.Rows(), maxRequestByteSize, encoder, func(chunk [][]byte) error {
		result, err := managedStream.AppendRows(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to append rows: %w", err)
		}

		if resp, err := result.FullResponse(ctx); err != nil {
			return fmt.Errorf("failed to get response (%s): %w", resp.GetError().String(), err)
		}

		return nil
	})
}

func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)

	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)

	defer func() { _ = ddl.DropTemporaryTable(s, stagingTableID, false) }()

	return destination.ExecStatements(s, dedupeQueries)
}

func LoadBigQuery(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
		}, nil
	}

	if credPath := cfg.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			return nil, fmt.Errorf("error setting env var for %q : %w", GooglePathToCredentialsEnvKey, err)
		}
	}

	store, err := db.Open("bigquery", cfg.BigQuery.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}, nil
}
