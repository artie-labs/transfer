package bigquery

import (
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
	"google.golang.org/protobuf/proto"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/batch"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	// Storage Write API is limited to 10 MiB, subtract 400 KiB to account for request overhead.
	maxRequestByteSize = (10 * 1024 * 1024) - (400 * 1024)
)

type Store struct {
	// If [auditRows] is enabled, we will perform an additional query to ensure that the number of rows in the temporary table matches the expected number of rows.
	auditRows bool
	configMap *types.DwhToTablesConfigMap
	config    config.Config

	db.Store
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	if !useTempTable {
		return shared.Append(ctx, s, tableData, types.AdditionalSettings{
			ColumnSettings: s.config.SharedDestinationSettings.ColumnSettings,
		})
	}

	// We can simplify this once Google has fully rolled out the ability to execute DML on recently streamed data
	// See: https://cloud.google.com/bigquery/docs/write-api#use_data_manipulation_language_dml_with_recently_streamed_data
	// For now, we'll need to append this to a temporary table and then append temporary table onto the target table
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	temporaryTableID := shared.TempTableID(tableID)

	defer func() { _ = ddl.DropTemporaryTable(s, temporaryTableID, false) }()

	err := shared.Append(ctx, s, tableData, types.AdditionalSettings{
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

	if _, err = s.Exec(query); err != nil {
		return fmt.Errorf("failed to insert data into target table: %w", err)
	}

	return nil
}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	bqTempTableID, err := typing.AssertType[dialect.TableIdentifier](tempTableID)
	if err != nil {
		return err
	}

	if err = s.putTable(ctx, bqTempTableID, tableData); err != nil {
		return fmt.Errorf("failed to put table: %w", err)
	}

	if s.auditRows {
		var tblRowCount int64
		if err = s.QueryRow(`SELECT COUNT(*) FROM %s`, tempTableID.FullyQualifiedName()).Scan(&tblRowCount); err != nil {
			return fmt.Errorf("failed to count rows in temporary table: %w", err)
		}

		expectedRowCount := int64(len(tableData.Rows()))
		// TableCount could be higher since AppendRows is at least once delivery.
		if tblRowCount >= expectedRowCount {
			return nil
		}

		return fmt.Errorf("temporary table row count mismatch, expected: %d, got: %d", expectedRowCount, tableCount)
	}

	return nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.config.BigQuery.ProjectID, topicConfig.Database, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    tableData.TopicConfig().DropDeletedColumns,
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
		message, err := rowToMessage(row, columns, *messageDescriptor)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row to message: %w", err)
		}

		bytes, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}

		return bytes, nil
	}

	return batch.BySize(tableData.Rows(), maxRequestByteSize, false, encoder, func(chunk [][]byte) error {
		result, err := managedStream.AppendRows(ctx, chunk)
		if err != nil {
			return fmt.Errorf("failed to append rows: %w", err)
		}

		resp, err := result.FullResponse(ctx)
		if err != nil {
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
}

func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)

	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)

	defer func() { _ = ddl.DropTemporaryTable(s, stagingTableID, false) }()

	return destination.ExecStatements(s, dedupeQueries)
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
	// BigQuery doesn't need to sweep temporary tables, since they support setting TTL on temporary tables.
	return nil
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

	var auditRows bool
	if val := os.Getenv("BQ_AUDIT_ROWS"); val != "" {
		auditRows, err = strconv.ParseBool(val)
		if err != nil {
			logger.Panic("Failed to parse BQ_AUDIT", slog.Any("err", err))
		}
	}

	return &Store{
		auditRows: auditRows,
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,

		Store: store,
	}, nil
}
