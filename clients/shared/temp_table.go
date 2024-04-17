package shared

import (
	"fmt"

	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TempTableName(dwh destination.DataWarehouse, tableData *optimization.TableData) string {
	return fmt.Sprintf(
		"%s_%s_%s_%d",
		dwh.ToFullyQualifiedName(tableData.TableIdentifier(), false),
		constants.ArtiePrefix,
		tableData.TempTableSuffix(),
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
}
