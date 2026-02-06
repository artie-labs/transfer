package mysql

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/artie-labs/transfer/clients/mysql/dialect"
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
	db.Store
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return true
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
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

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.MySQLDialect {
	return dialect.MySQLDialect{}
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

// specificIdentifierFor returns a MySQL [TableIdentifier] for a [TopicConfig] + table name.
func (s *Store) specificIdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) dialect.TableIdentifier {
	// MySQL uses database instead of schema, so we use the database from the config
	return dialect.NewTableIdentifier(databaseAndSchema.Schema, table)
}

// IdentifierFor returns a generic [sql.TableIdentifier] interface for a [TopicConfig] + table name.
func (s *Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(databaseAndSchema, table)
}

func (s *Store) SweepTemporaryTables(ctx context.Context, whClient *webhooksclient.Client) error {
	return shared.Sweep(ctx, s, s.config.TopicConfigs(), whClient, s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(_ context.Context, _ sql.TableIdentifier, _ kafkalib.DatabaseAndSchemaPair, _ []string, _ bool) error {
	return nil // dedupe is not necessary for MySQL
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

func LoadStore(cfg config.Config) (*Store, error) {
	store, err := db.Open("mysql", cfg.MySQL.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}
