package databricks

import (
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
	cfg config.Config
}

func (s Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, types.MergeOpts{})
}

func (s Store) Append(tableData *optimization.TableData, useTempTable bool) error {
	return shared.Append(s, tableData, types.AdditionalSettings{UseTempTable: useTempTable})
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	//return NewTableIdentifier(topicConfig.Schema, table)
}

func (s Store) Dialect() sql.Dialect {
	//return dialect.DatabricksDialect{}
}

func (s Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	//return shared.Dedupe(s, tableID, primaryKeys, includeArtieUpdatedAt)
}

func (s Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	//query, args := describeTableQuery(tableID)
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		Query:                 query,
		Args:                  args,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	//return shared.PrepareTemporaryTable(s, tableData, tableConfig, tempTableID, parentTableID, additionalSettings, createTempTable)
}

func LoadStore(cfg config.Config) (Store, error) {
	store, err := db.Open("databricks", cfg.Databricks.DSN())
	if err != nil {
		return Store{}, err
	}
	return Store{
		Store: store,
		cfg:   cfg,
	}, nil
}
