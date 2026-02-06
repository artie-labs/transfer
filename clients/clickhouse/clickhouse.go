package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/artie-labs/transfer/clients/clickhouse/dialect"
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
	db.Store
	configMap *types.DestinationTableConfigMap
	config    config.Config
}

func LoadClickhouse(ctx context.Context, cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store:     *_store,
			configMap: &types.DestinationTableConfigMap{},
			config:    cfg,
		}, nil
	}

	if cfg.Clickhouse == nil {
		return nil, fmt.Errorf("clickhouse config is nil")
	}

	var tlsConfig *tls.Config
	if !cfg.Clickhouse.IsInsecure {
		tlsConfig = &tls.Config{}
	}

	store := db.NewStoreWrapper(clickhouse.OpenDB(&clickhouse.Options{
		Addr: cfg.Clickhouse.Addresses,
		Auth: clickhouse.Auth{
			Username: cfg.Clickhouse.Username,
			Password: cfg.Clickhouse.Password,
		},
		TLS: tlsConfig,
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{
					Name:    "artie-transfer",
					Version: "1.0.0",
				},
			},
		},
	}))

	if err := store.GetDatabase().Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}

func (s Store) Dialect() sql.Dialect {
	return dialect.ClickhouseDialect{}
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return false
}

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "name",
		ColumnNameForDataType: "type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig(ctx)
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Database, table)
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, useTempTable bool) error {
	return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{})
}

func (s Store) IsRetryableError(err error) bool {
	return false
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	err := shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{})
	if err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, _ kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	return nil
}

func (s Store) SweepTemporaryTables(ctx context.Context, whClient *webhooksclient.Client) error {
	return shared.Sweep(ctx, s, s.config.TopicConfigs(), whClient, dialect.ClickhouseDialect{}.BuildSweepQuery)
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
