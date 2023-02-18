package ddl

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
)

func AlterTable(_ context.Context, dwh dwh.DataWarehouse, tc *types.DwhTableConfig, fqTableName string, createTable bool, columnOp constants.ColumnOperation, cdcTime time.Time, cols ...typing.Column) error {
	var mutateCol []typing.Column
	var colSQLPart string
	var err error
	for _, col := range cols {
		if col.Kind == typing.Invalid {
			// Let's not modify the table if the column kind is invalid
			continue
		}

		if columnOp == constants.Delete && !tc.ShouldDeleteColumn(col.Name, cdcTime) {
			// Don't delete yet, we can evaluate when we consume more messages.
			continue
		}

		mutateCol = append(mutateCol, col)
		switch columnOp {
		case constants.Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToDWHType(col.Kind, dwh.Label()))
		case constants.Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, columnOp, colSQLPart)
		if createTable {
			sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, colSQLPart)
			createTable = false
		}

		_, err = dwh.Exec(sqlQuery)
		if err != nil && ColumnAlreadyExistErr(err, dwh.Label()) {
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		tc.MutateInMemoryColumns(createTable, columnOp, mutateCol...)
	}

	return nil
}
