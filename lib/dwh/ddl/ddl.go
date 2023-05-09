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

type AlterTableArgs struct {
	Dwh            dwh.DataWarehouse
	Tc             *types.DwhTableConfig
	FqTableName    string
	CreateTable    bool
	TemporaryTable bool
	ColumnOp       constants.ColumnOperation

	CdcTime time.Time
}

func AlterTable(_ context.Context, args AlterTableArgs, cols ...typing.Column) error {
	var mutateCol []typing.Column
	var colSQLPart string
	var err error
	for _, col := range cols {
		if col.KindDetails == typing.Invalid {
			// Let's not modify the table if the column kind is invalid
			continue
		}

		if args.ColumnOp == constants.Delete && !args.Tc.ShouldDeleteColumn(col.Name, args.CdcTime) {
			// Don't delete yet, we can evaluate when we consume more messages.
			continue
		}

		mutateCol = append(mutateCol, col)
		switch args.ColumnOp {
		case constants.Add:
			colSQLPart = fmt.Sprintf("%s %s", col.Name, typing.KindToDWHType(col.KindDetails, args.Dwh.Label()))
		case constants.Delete:
			colSQLPart = fmt.Sprintf("%s", col.Name)
		}

		// If the table does not exist, create it.
		sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", args.FqTableName, args.ColumnOp, colSQLPart)
		if args.CreateTable {
			if args.TemporaryTable {
				// Snowflake has this feature too, but we don't need it as our CTE approach with Snowflake is extremely performant.
				if args.Dwh.Label() != constants.BigQuery {
					return fmt.Errorf("unexpected temporary table for destination: %v", args.Dwh.Label())
				}

				// https://cloud.google.com/bigquery/docs/multi-statement-queries#create_temporary_table
				sqlQuery = fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (%s)", args.FqTableName, colSQLPart)
			} else {
				sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", args.FqTableName, colSQLPart)
			}
			args.CreateTable = false
		}

		_, err = args.Dwh.Exec(sqlQuery)
		if err != nil && ColumnAlreadyExistErr(err, args.Dwh.Label()) {
			err = nil
		} else if err != nil {
			return err
		}
	}

	if err == nil {
		args.Tc.MutateInMemoryColumns(args.CreateTable, args.ColumnOp, mutateCol...)
	}

	return nil
}
