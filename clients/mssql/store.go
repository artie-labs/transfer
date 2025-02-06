package mssql

import (
	"context"
	"fmt"
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
	configMap *types.DestinationTableConfigMap
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

func (s *Store) DropTable(_ context.Context, _ sql.TableIdentifier) error {
	return fmt.Errorf("not supported")
}

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.MSSQLDialect {
	return dialect.MSSQLDialect{}
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

// specificIdentifierFor returns a MS SQL [TableIdentifier] for a [TopicConfig] + table name.
func (s *Store) specificIdentifierFor(topicConfig kafkalib.TopicConfig, table string) dialect.TableIdentifier {
	return dialect.NewTableIdentifier(getSchema(topicConfig.Schema), table)
}

// IdentifierFor returns a generic [sql.TableIdentifier] interface for a [TopicConfig] + table name.
func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(topicConfig, table)
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	return shared.Sweep(s, tcs, s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(_ sql.TableIdentifier, _ []string, _ bool) error {
	return nil // dedupe is not necessary for MS SQL
}

func (s *Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig()
}

func LoadStore(cfg config.Config) (*Store, error) {
	store, err := db.Open("mssql", cfg.MSSQL.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}
