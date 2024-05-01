package destination

import (
	"database/sql"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
)

type DataWarehouse interface {
	Label() constants.DestinationKind
	Dialect() sqllib.Dialect
	Merge(tableData *optimization.TableData) error
	Append(tableData *optimization.TableData) error
	Dedupe(tableID types.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) error
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)

	// Helper functions for merge
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier
	GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error)
	PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID types.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

type Baseline interface {
	Label() constants.DestinationKind
	Merge(tableData *optimization.TableData) error
	Append(tableData *optimization.TableData) error
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier
}
