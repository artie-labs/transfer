package postgresql

import (
	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/config"
	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	config    config.Config

	db.Store
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.PostgreSQL
}

const (
	describeNameCol        = "column_name"
	describeTypeCol        = "data_type"
	describeDescriptionCol = "description"
)

func (s *Store) getTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	describeQuery, err := describeTableQuery(describeArgs{
		RawTableName: tableData.RawName(),
		Schema:       tableData.TopicConfig.Schema,
	})

	if err != nil {
		return nil, err
	}

	return utils.GetTableConfig(utils.GetTableCfgArgs{
		Dwh:             s,
		FqName:          tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, ""),
		ConfigMap:       s.configMap,
		Query:           describeQuery,
		ColumnNameLabel: describeNameCol,
		ColumnTypeLabel: describeTypeCol,
		ColumnDescLabel: describeDescriptionCol,

		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
}

func LoadPostgreSQL(cfg config.Config, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
			Store:     *_store,
		}
	}

	return &Store{
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
		Store:     db.Open("postgres", cfg.PostgreSQL.ConnectionString()),
	}
}
