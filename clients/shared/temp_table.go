package shared

import (
	"fmt"

	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TempTableName(dwh destination.DataWarehouse, tableID optimization.TableIdentifier, suffix string) string {
	return fmt.Sprintf(
		"%s_%s_%s_%d",
		dwh.ToFullyQualifiedName(tableID, false),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
}
