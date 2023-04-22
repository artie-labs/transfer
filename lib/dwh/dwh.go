package dwh

import (
	"context"
	"database/sql"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/models/flush"
)

type DataWarehouse interface {
	Label() constants.DestinationKind
	Merge(ctx context.Context, tableData *flush.TableData) error
	Exec(query string, args ...any) (sql.Result, error)
}
