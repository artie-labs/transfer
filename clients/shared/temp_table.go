package shared

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func TempTableID(tableID sql.TableIdentifier) sql.TableIdentifier {
	return TempTableIDWithSuffix(tableID, strings.ToLower(stringutil.Random(5)))
}

func TempTableIDWithSuffix(tableID sql.TableIdentifier, suffix string) sql.TableIdentifier {
	tempTable := fmt.Sprintf(
		"%s_%s_%s_%d",
		tableID.Table(),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
	return tableID.WithTable(tempTable)
}
