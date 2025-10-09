package postgres

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"

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
	store, err := db.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		return nil, err
	}

	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}

func (s Store) dialect() dialect.PostgresDialect {
	return dialect.PostgresDialect{}
}

func (s Store) Dialect() sql.Dialect {
	return s.dialect()
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
	return shared.Sweep(ctx, s, s.config.TopicConfigs(), s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	return fmt.Errorf("dedupe not implemented for PostgreSQL")
}

func (s *Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool, originalTableName string) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		DropDeletedColumns:    dropDeletedColumns,
		OriginalTableName:     originalTableName,
	}.GetTableConfig(ctx)
}
