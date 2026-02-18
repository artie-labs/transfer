package bigquery

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	_ "github.com/viant/bigquery"
	"google.golang.org/api/option"
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
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	maxRequestBytesSize int
	configMap           *types.DestinationTableConfigMap
	config              config.Config
	bqClient            *bigquery.Client

	db.Store
}

func (s Store) Label() constants.DestinationKind {
	return s.config.Output
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return false
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	return shared.DropTemporaryTable(ctx, s, tableID, s.configMap)
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool) error {
	if !useTempTable {
		return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{
			ColumnSettings: s.config.SharedDestinationSettings.ColumnSettings,
		})
	}

	// We can simplify this once Google has fully rolled out the ability to execute DML on recently streamed data
	// See: https://cloud.google.com/bigquery/docs/write-api#use_data_manipulation_language_dml_with_recently_streamed_data
	// For now, we'll need to append this to a temporary table and then append temporary table onto the target table
	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	temporaryTableID := shared.TempTableID(s, tableID)

	defer func() { _ = ddl.DropTemporaryTable(ctx, s, temporaryTableID, false) }()

	err := shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{
		ColumnSettings: s.config.SharedDestinationSettings.ColumnSettings,
		UseTempTable:   true,
		TempTableID:    temporaryTableID,
	})
	if err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s`,
		tableID.FullyQualifiedName(),
		strings.Join(sql.QuoteColumns(tableData.ReadOnlyInMemoryCols().ValidColumns(), s.Dialect()), ","),
		strings.Join(sql.QuoteColumns(tableData.ReadOnlyInMemoryCols().ValidColumns(), s.Dialect()), ","),
		temporaryTableID.FullyQualifiedName(),
	)

	if _, err = s.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to insert data into target table: %w", err)
	}

	return nil
}

func (s *Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return err
		}
	}

	bqTempTableID, err := typing.AssertType[dialect.TableIdentifier](tableID)
	if err != nil {
		return err
	}

	if err = s.putTable(ctx, bqTempTableID, tableData); err != nil {
		return fmt.Errorf("failed to put table: %w", err)
	}

	return nil
}

func (s *Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.config.BigQuery.ProjectID, databaseAndSchema.Database, table)
}

func (s *Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig(ctx)
}

func (s *Store) GetConfigMap() *types.DestinationTableConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return dialect.BigQueryDialect{}
}

func (s *Store) GetClient(ctx context.Context) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, s.config.BigQuery.ProjectID)
	if err != nil {
		logger.Panic("Failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) putTable(ctx context.Context, bqTableID dialect.TableIdentifier, tableData *optimization.TableData) error {
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

	// Create a committed stream for exactly-once semantics
	managedStream, err := managedWriterClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(
			managedwriter.TableParentFromParts(bqTableID.ProjectID(), bqTableID.Dataset(), bqTableID.Table()),
		),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(schemaDescriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return fmt.Errorf("failed to create managed stream: %w", err)
	}
	defer managedStream.Close()

	encoder := func(row optimization.Row) ([]byte, error) {
		message, err := rowToMessage(row.GetData(), columns, *messageDescriptor, s.config)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row to message: %w", err)
		}

		bytes, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}

		return bytes, nil
	}

	skipped, err := batch.BySize(tableData.Rows(), s.maxRequestBytesSize, false, encoder, func(chunk [][]byte, _ []optimization.Row) error {
		result, err := managedStream.AppendRows(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to append rows: %w", err)
		}

		resp, err := result.FullResponse(ctx)
		if err != nil {
			if resp != nil {
				if rowErrs := resp.GetRowErrors(); len(rowErrs) > 0 {
					// Just log the first few errors
					var errors []any
					for i, rowErr := range rowErrs {
						if i > 5 {
							break
						}

						errors = append(errors, rowErr)
					}

					return fmt.Errorf("failed to append rows, encountered %d errors: %v", len(rowErrs), errors)
				}
			}

			return fmt.Errorf("failed to get response: %w", err)
		}

		if status := resp.GetError(); status != nil {
			return fmt.Errorf("failed to append rows: %s", status.String())
		}

		if rowErrs := resp.GetRowErrors(); len(rowErrs) > 0 {
			return fmt.Errorf("failed to append rows, encountered %d errors", len(rowErrs))
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to write rows: %w", err)
	}

	// Get the final row count from the stream
	rowCount, err := managedStream.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("failed to finalize stream: %w", err)
	}

	// Verify that we wrote all expected rows
	expectedRows := uint64(int(tableData.NumberOfRows()) - skipped)
	if uint64(rowCount) != expectedRows {
		return fmt.Errorf("row count mismatch after write, expected: %d, got: %d", expectedRows, rowCount)
	}

	return nil
}

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)

	defer func() { _ = ddl.DropTemporaryTable(ctx, s, stagingTableID, false) }()

	if _, err := destination.ExecContextStatements(ctx, s, dedupeQueries); err != nil {
		return fmt.Errorf("failed to dedupe: %w", err)
	}

	return nil
}

func (s *Store) SweepTemporaryTables(_ context.Context, _ *webhooksclient.Client) error {
	// BigQuery doesn't need to sweep temporary tables, since they support setting TTL on temporary tables.
	return nil
}

func LoadStore(ctx context.Context, cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			configMap: &types.DestinationTableConfigMap{},
			config:    cfg,
		}, nil
	}

	if credPath := cfg.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to BQ credentials to env var for google auth")
		if err := os.Setenv(GooglePathToCredentialsEnvKey, credPath); err != nil {
			return nil, fmt.Errorf("error setting env var for %q : %w", GooglePathToCredentialsEnvKey, err)
		}
	}

	bqClient, err := bigquery.NewClient(ctx, cfg.BigQuery.ProjectID,
		option.WithCredentialsFile(os.Getenv(GooglePathToCredentialsEnvKey)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	store, err := db.Open("bigquery", cfg.BigQuery.DSN())
	if err != nil {
		return nil, err
	}

	// Default to using 90% of the 10 MiB limit to account for gRPC request overhead
	// (headers, protobuf envelope, metadata, field tags, etc).
	// You can override this by setting [BIGQUERY_STORAGE_WRITE_MAX_PERCENT] (1-100).
	maxPercent, err := strconv.Atoi(cmp.Or(os.Getenv("BIGQUERY_STORAGE_WRITE_MAX_PERCENT"), "90"))
	if err != nil {
		return nil, fmt.Errorf("failed to convert max percent to int: %w", err)
	}

	if maxPercent < 1 || maxPercent > 100 {
		return nil, fmt.Errorf("BIGQUERY_STORAGE_WRITE_MAX_PERCENT must be between 1 and 100, got: %d", maxPercent)
	}

	// Storage Write API is limited to 10 MiB. Use percentage to leave room for request overhead.
	const bigQueryMaxRequestSize = 10 * 1024 * 1024
	maxRequestByteSize := int(float64(bigQueryMaxRequestSize) * float64(maxPercent) / 100.0)
	slog.Info("Loaded BigQuery storage write configuration",
		slog.Int("maxPercent", maxPercent),
		slog.Int("maxPayloadBytes", maxRequestByteSize),
		slog.Int("overheadBytes", bigQueryMaxRequestSize-maxRequestByteSize),
	)
	return &Store{
		bqClient:            bqClient,
		configMap:           &types.DestinationTableConfigMap{},
		config:              cfg,
		Store:               store,
		maxRequestBytesSize: maxRequestByteSize,
	}, nil
}
