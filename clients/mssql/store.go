package mssql

import (
	"strings"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/clients/mssql/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	config    config.Config
	db.Store
}

func getSchema(schema string) string {
	// MSSQL has their default schema called `dbo`, `public` is a reserved keyword.
	if strings.ToLower(schema) == "public" {
		return "dbo"
	}

	return schema
}

func (s *Store) Dialect() sql.Dialect {
	return dialect.MSSQLDialect{}
}

func (s *Store) AdditionalDateFormats() []string {
	return s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, types.MergeOpts{})
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, types.AdditionalSettings{})
}

// specificIdentifierFor returns a MS SQL [TableIdentifier] for a [TopicConfig] + table name.
func (s *Store) specificIdentifierFor(topicConfig kafkalib.TopicConfig, table string) TableIdentifier {
	return NewTableIdentifier(getSchema(topicConfig.Schema), table)
}

// IdentifierFor returns a generic [sql.TableIdentifier] interface for a [TopicConfig] + table name.
func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(topicConfig, table)
}

func (s *Store) Sweep() error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	queryFunc := func(dbAndSchemaPair kafkalib.DatabaseSchemaPair) (string, []any) {
		return sweepQuery(getSchema(dbAndSchemaPair.Schema))
	}

	return shared.Sweep(s, tcs, queryFunc)
}

func (s *Store) Dedupe(_ sql.TableIdentifier, _ []string, _ kafkalib.TopicConfig) error {
	return nil // dedupe is not necessary for MS SQL
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	tableID := s.specificIdentifierFor(tableData.TopicConfig(), tableData.Name())
	query, args := describeTableQuery(tableID)
	return shared.GetTableCfgArgs{
		Dwh:                      s,
		TableID:                  tableID,
		ConfigMap:                s.configMap,
		Query:                    query,
		Args:                     args,
		ColumnNameForName:        "column_name",
		ColumnNameForDataType:    "data_type",
		ColumnNameForDescription: "column_default",
		DropDeletedColumns:       tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func LoadStore(cfg config.Config) (*Store, error) {
	store, err := db.Open("mssql", cfg.MSSQL.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}, nil
}
