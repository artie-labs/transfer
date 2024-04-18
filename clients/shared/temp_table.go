package shared

import (
	"fmt"

	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
)

func TempTableName(dwh destination.DataWarehouse, tableID types.TableIdentifier, suffix string) string {
	return fmt.Sprintf(
		"%s_%s_%s_%d",
		tableID.FullyQualifiedName(false, dwh.ShouldUppercaseEscapedNames()),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
}
