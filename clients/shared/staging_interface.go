package shared

import (
	"context"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type ReusableStagingTableManager interface {
	CheckStagingTableExists(ctx context.Context, tableID sql.TableIdentifier) (bool, error)

	ValidateStagingTableSchema(ctx context.Context, tableID sql.TableIdentifier, expectedColumns []columns.Column) (bool, error)

	TruncateStagingTable(ctx context.Context, tableID sql.TableIdentifier) error

	HandleStagingTableSchemaChange(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier) error

	StagingTableHasData(ctx context.Context, tableID sql.TableIdentifier) (bool, error)

	MergeStagingDataToTarget(ctx context.Context, stagingTableID sql.TableIdentifier, targetTableID sql.TableIdentifier, tableData *optimization.TableData) error

	AlterStagingTableSchema(ctx context.Context, tableID sql.TableIdentifier, newColumns []columns.Column) error
}
