package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func GenerateReusableStagingTableName(baseTableName, suffix string) string {
	return fmt.Sprintf("%s_%s_%s", baseTableName, constants.ArtiePrefix, suffix)
}

type ReusableStagingTableManager interface {
	PrepareReusableStagingTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, stagingTableID, parentTableID sql.TableIdentifier, opts types.AdditionalSettings) error

	CheckStagingTableExists(ctx context.Context, tableID sql.TableIdentifier) (bool, error)

	ValidateStagingTableSchema(ctx context.Context, tableID sql.TableIdentifier, expectedColumns []columns.Column) (bool, error)
}
