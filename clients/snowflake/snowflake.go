package snowflake

import (
	"context"
	"fmt"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	db.Store
	configMap *types.DwhToTablesConfigMap
	config    config.Config
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:             s.configMap,
		ColumnNameForName:     "name",
		ColumnNameForDataType: "type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	return shared.Sweep(s, tcs, s.dialect().BuildSweepQuery)
}

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.SnowflakeDialect {
	return dialect.SnowflakeDialect{}
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

// Dedupe takes a table and will remove duplicates based on the primary key(s).
// These queries are inspired and modified from: https://stackoverflow.com/a/71515946
func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	return destination.ExecStatements(s, dedupeQueries)
}

func LoadSnowflake(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
			Store:     *_store,
		}, nil
	}

	snowflakeCfg, err := cfg.Snowflake.ToConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Snowflake config: %w", err)
	}

	dsn, err := gosnowflake.DSN(snowflakeCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get Snowflake DSN: %w", err)
	}

	store, err := db.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	return &Store{
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
		Store:     store,
	}, nil
}
