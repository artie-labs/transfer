package mssql

import (
	"strings"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
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

func (s *Store) Label() constants.DestinationKind {
	return constants.MSSQL
}

func (s *Store) ShouldUppercaseEscapedNames() bool {
	return s.config.SharedDestinationConfig.UppercaseEscapedNames
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, s.config, types.MergeOpts{})
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, s.config, types.AppendOpts{
		TempTableName: s.ToFullyQualifiedName(tableData, true),
	})
}

func (s *Store) identifierFor(topicConfig kafkalib.TopicConfig, table string) TableIdentifier {
	return NewTableIdentifier(getSchema(topicConfig.Schema), table)
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier {
	return s.identifierFor(topicConfig, table)
}

func (s *Store) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	tableID := s.IdentifierFor(tableData.TopicConfig, tableData.Name())
	return tableID.FullyQualifiedName(escape, s.ShouldUppercaseEscapedNames())
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

func (s *Store) Dedupe(_ types.TableIdentifier) error {
	return nil // dedupe is not necessary for MS SQL
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	// TODO: Figure out how to leave a comment.
	const (
		describeNameCol        = "column_name"
		describeTypeCol        = "data_type"
		describeDescriptionCol = "description"
	)

	query, args := describeTableQuery(s.identifierFor(tableData.TopicConfig, tableData.Name()))
	return shared.GetTableCfgArgs{
		Dwh:                s,
		FqName:             s.ToFullyQualifiedName(tableData, true),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               args,
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
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
