package mssql

import (
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	_ "github.com/microsoft/go-mssqldb"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	config    config.Config
	db.Store
}

func (s *Store) Schema(tableData *optimization.TableData) string {
	// MSSQL has their default schema called `dbo`, `public` is a reserved keyword.
	if strings.ToLower(tableData.TopicConfig.Schema) == "public" {
		return "dbo"
	}

	return tableData.TopicConfig.Schema
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

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	// TODO: Figure out how to leave a comment.

	const (
		describeNameCol        = "column_name"
		describeTypeCol        = "data_type"
		describeDescriptionCol = "description"
	)

	describeQuery, err := describeTableQuery(describeArgs{
		RawTableName: tableData.RawName(),
		Schema:       s.Schema(tableData),
	})

	if err != nil {
		return nil, err
	}

	return shared.GetTableConfig(shared.GetTableCfgArgs{
		Dwh:                s,
		FqName:             s.ToFullyQualifiedName(tableData, true),
		ConfigMap:          s.configMap,
		Query:              describeQuery,
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
}

func LoadStore(cfg config.Config, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			Store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
		}
	}

	return &Store{
		Store:     db.Open("mssql", cfg.MSSQL.DSN()),
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}
}
