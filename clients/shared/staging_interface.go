package shared

import (
	"context"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type ReusableStagingTableManager interface {
	PrepareReusableStagingTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, stagingTableID sql.TableIdentifier, parentTableID sql.TableIdentifier) error

	CheckStagingTableExists(ctx context.Context, tableID sql.TableIdentifier) (bool, error)

	ValidateStagingTableSchema(ctx context.Context, tableID sql.TableIdentifier, expectedColumns []columns.Column) (bool, error)

	TruncateStagingTable(ctx context.Context, tableID sql.TableIdentifier) error
}
