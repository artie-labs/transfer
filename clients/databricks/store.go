package databricks

import (
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
	//TODO implement me
	panic("implement me")
}

func (s Store) Append(tableData *optimization.TableData, useTempTable bool) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	//TODO implement me
	panic("implement me")
}

func (s Store) Dialect() sql.Dialect {
	//TODO implement me
	panic("implement me")
}

func (s Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	//TODO implement me
	panic("implement me")
}

func LoadStore(cfg config.Config) (Store, error) {
	return Store{cfg: cfg}, nil
}
