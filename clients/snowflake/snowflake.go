package snowflake

import (
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
	"github.com/artie-labs/transfer/lib/typing"
)

type Store struct {
	db.Store
	testDB    bool // Used for testing
	configMap *types.DwhToTablesConfigMap
	config    config.Config
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		Query:                 fmt.Sprintf("DESC TABLE %s;", tableID.FullyQualifiedName()),
		ColumnNameForName:     "name",
		ColumnNameForDataType: "type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) Sweep() error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	snowflakeDialect, err := typing.AssertType[dialect.SnowflakeDialect](s.Dialect())
	if err != nil {
		return err
	}

	return shared.Sweep(s, tcs, snowflakeDialect.BuildSweepQuery)
}

func (s *Store) Dialect() sql.Dialect {
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
			testDB:    true,
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
