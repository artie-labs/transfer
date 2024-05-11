package shared

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func TempTableID(tableID sql.TableIdentifier, suffix string) sql.TableIdentifier {
	tempTable := fmt.Sprintf(
		"%s_%s_%s_%d",
		tableID.Table(),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
	return tableID.WithTable(tempTable)
}
