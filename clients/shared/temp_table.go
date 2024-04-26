package shared

import (
	"fmt"

	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
)

func TempTableID(tableID types.TableIdentifier, suffix string) types.TableIdentifier {
	tempTable := fmt.Sprintf(
		"%s_%s_%s_%d",
		tableID.Table(),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
	return tableID.WithTable(tempTable)
}
