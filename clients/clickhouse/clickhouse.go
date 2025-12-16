package clickhouse

import (
	"context"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/artie-labs/transfer/clients/clickhouse/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	db.Store
	configMap *types.DestinationTableConfigMap
	config    config.Config
}

func LoadClickhouse(ctx context.Context, cfg config.Config, _store *db.Store) (*Store, error) {
	if cfg.Clickhouse == nil {
		return nil, fmt.Errorf("clickhouse config is nil")
	}

	store, err := db.Open("clickhouse", fmt.Sprintf("clickhouse://%s?username=%s&password=%s", cfg.Clickhouse.Address, cfg.Clickhouse.Username, cfg.Clickhouse.Password))
	if err != nil {
		return &Store{}, err
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

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return nil, nil
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Schema, table)
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{})
}

func (s Store) IsRetryableError(err error) bool {
	return false
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	return nil
}

func (s Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tableID, parentTableID sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	return nil
}

func (s Store) SweepTemporaryTables(ctx context.Context) error {
	return nil
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	return nil
}
