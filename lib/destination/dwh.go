package destination

import (
	"database/sql"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
)

type DataWarehouse interface {
	Label() constants.DestinationKind
	Merge(tableData *optimization.TableData) error
	Append(tableData *optimization.TableData) error
	Dedupe(tableID types.TableIdentifier) error
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)

	// Helper functions for merge
	ShouldUppercaseEscapedNames() bool
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier
	GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error)
	PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings, createTempTable bool) error
}

type Baseline interface {
	Label() constants.DestinationKind
	Merge(tableData *optimization.TableData) error
	Append(tableData *optimization.TableData) error
	IsRetryableError(err error) bool
	IdentifierFor(topicConfig kafkalib.TopicConfig, table string) types.TableIdentifier
}
