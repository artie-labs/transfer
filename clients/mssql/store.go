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

func (s *Store) Schema(tableData *optimization.TableData) string {
	return getSchema(tableData.TopicConfig.Schema)
}

func (s *Store) Label() constants.DestinationKind {
	return constants.MSSQL
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, s.config, types.MergeOpts{})
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, s.config, types.AppendOpts{
		TempTableName: s.ToFullyQualifiedName(tableData, true),
	})
}

func (s *Store) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	return tableData.ToFqName(s.Label(), escape, s.config.SharedDestinationConfig.UppercaseEscapedNames, optimization.FqNameOpts{
		MsSQLSchemaOverride: s.Schema(tableData),
	})
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

func (s *Store) Dedupe(fqTableName string) error {
	return nil // dedupe is not necessary for MS SQL
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	// TODO: Figure out how to leave a comment.
	const (
		describeNameCol        = "column_name"
		describeTypeCol        = "data_type"
		describeDescriptionCol = "description"
	)

	query, args := describeTableQuery(s.Schema(tableData), tableData.RawName())
	return shared.GetTableConfig(shared.GetTableCfgArgs{
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
	})
}

func LoadStore(cfg config.Config) *Store {
	return &Store{
		Store:     db.Open("mssql", cfg.MSSQL.DSN()),
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}
}
