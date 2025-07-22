package postgres

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	configMap *types.DestinationTableConfigMap
	config    config.Config

	db.Store
}

func LoadStore(cfg config.Config) (*Store, error) {
	store, err := db.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return nil, err
	}

	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}

func (s Store) Dialect() sql.Dialect {
	return dialect.PostgresDialect{}
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.AllowToDrop() {
		return fmt.Errorf("table %q is not allowed to be dropped", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.Dialect().BuildDropTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// We'll then clear it from our cache
	s.configMap.RemoveTable(tableID)
	return nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{})
}

// specificIdentifierFor returns a PostgreSQL [TableIdentifier] for a [TopicConfig] + table name.
func (s *Store) specificIdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) dialect.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Schema, table)
}

// IdentifierFor returns a generic [sql.TableIdentifier] interface for a [TopicConfig] + table name.
func (s *Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(databaseAndSchema, table)
}

func (s *Store) SweepTemporaryTables(ctx context.Context) error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	// TODO: Implement PostgreSQL-specific sweep query
	// For now, use a placeholder that won't find any tables
	buildSweepQuery := func(dbName string, schemaName string) (string, []any) {
		// TODO: Implement proper PostgreSQL sweep query for temporary tables
		return "SELECT schemaname, tablename FROM pg_tables WHERE false", nil
	}

	return shared.Sweep(ctx, s, tcs, buildSweepQuery)
}

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	// TODO: Implement PostgreSQL-specific dedupe logic
	return fmt.Errorf("dedupe not implemented for PostgreSQL")
}

func (s *Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "column_comment",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, tableConfig, opts.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	// TODO: Implement PostgreSQL-specific data loading into temporary table
	// This would typically involve generating INSERT statements or using COPY FROM
	return fmt.Errorf("PrepareTemporaryTable not fully implemented for PostgreSQL")
}
