package sql_server

import (
	_ "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	config    config.Config
	db.Store
}

func (s *Store) Label() constants.DestinationKind {
	return constants.SQLServer
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return nil
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return nil
}

func (s *Store) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	return ""
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	return nil, nil
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings) error {
	return nil
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
		Store:     db.Open("mssql", cfg.SQLServer.DSN()),
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}
}
