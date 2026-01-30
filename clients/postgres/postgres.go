package postgres

import (
	"context"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

type Store struct {
	configMap *types.DestinationTableConfigMap
	config    config.Config
	version   int

	db.Store
}

func LoadStore(ctx context.Context, cfg config.Config) (*Store, error) {
	store, err := db.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		return nil, err
	}

	out, err := db.RetrieveVersion(ctx, store.GetDatabase())
	if err != nil {
		if closeErr := store.Close(); closeErr != nil {
			slog.Warn("Failed to close database after error", slog.Any("error", closeErr))
		}

		return nil, fmt.Errorf("failed to retrieve version: %w", err)
	}

	slog.Info("Loaded Postgres as a destination", slog.Int("version", out.Major))
	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
		version:   out.Major,
	}, nil
}

func (s Store) dialect() dialect.PostgresDialect {
	// https://www.postgresql.org/docs/current/sql-merge.html
	return dialect.NewPostgresDialect(s.version < 15)
}

func (s Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.TemporaryTable() {
		return fmt.Errorf("table %q is not a temporary table, so it cannot be dropped", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.Dialect().BuildDropTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// We'll then clear it from our cache
	s.configMap.RemoveTable(tableID)
	return nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}, whClient); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{})
}

// specificIdentifierFor returns a PostgreSQL [TableIdentifier] for a [TopicConfig] + table name.
func (s *Store) specificIdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) dialect.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Schema, table)
}

// IdentifierFor returns a generic [sql.TableIdentifier] interface for a [TopicConfig] + table name.
func (s *Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(databaseAndSchema, table)
}

func (s *Store) SweepTemporaryTables(ctx context.Context, whClient *webhooksclient.Client) error {
	return shared.Sweep(ctx, s, s.config.TopicConfigs(), whClient, s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	return fmt.Errorf("dedupe not implemented for PostgreSQL")
}

func (s *Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig(ctx)
}
