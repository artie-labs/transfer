package ddl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
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

func DropTemporaryTable(ctx context.Context, dwh dwh.DataWarehouse, fqTableName string) {
	if strings.Contains(fqTableName, constants.ArtiePrefix) {
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#drop_table_statement
		// https://docs.snowflake.com/en/sql-reference/sql/drop-table
		_, err := dwh.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", fqTableName))
		if err != nil {
			logger.FromContext(ctx).WithError(err).Warn("failed to drop temporary table, it will get garbage collected by the TTL...")
		}
	} else {
		logger.FromContext(ctx).Warn(fmt.Sprintf("skipped dropping table: %s because it does not contain the artie prefix", fqTableName))
	}
	return
}

func AlterTable(_ context.Context, args AlterTableArgs, cols ...typing.Column) error {
	// You can't DROP a column and try to create a table at the same time.
	if args.ColumnOp == constants.Delete && args.CreateTable {
		return fmt.Errorf("incompatiable operation - cannot drop columns and create table at the asme time, args: %v", args)
	}

	var mutateCol []typing.Column
	// It's okay to combine since args.ColumnOp only takes one of: `Delete` or `Add`
	var colSQLParts []string
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
			colSQLParts = append(colSQLParts, fmt.Sprintf("%s %s", col.Name, typing.KindToDWHType(col.KindDetails, args.Dwh.Label())))
		case constants.Delete:
			colSQLParts = append(colSQLParts, fmt.Sprintf("%s", col.Name))
		}
	}

	var err error
	if args.CreateTable {
		var sqlQuery string
		if args.TemporaryTable {
			switch args.Dwh.Label() {
			case constants.BigQuery:
				expiry := time.Now().UTC().Add(constants.BigQueryTempTableTTL)
				sqlQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
					args.FqTableName, strings.Join(colSQLParts, ","), typing.BigQueryDate(expiry))
			case constants.Snowflake:
				break
			default:
				return fmt.Errorf("unexpected dwh: %v trying to create a temporary table", args.Dwh.Label())
			}

		} else {
			sqlQuery = fmt.Sprintf(`CREATE TABLE  %s (%s) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' );`, args.FqTableName, strings.Join(colSQLParts, ","))
		}

		_, err = args.Dwh.Exec(sqlQuery)
		if ColumnAlreadyExistErr(err, args.Dwh.Label()) {
			err = nil
		} else if err != nil {
			return err
		}
	} else {
		for _, colSQLPart := range colSQLParts {
			sqlQuery := fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", args.FqTableName, args.ColumnOp, colSQLPart)
			_, err = args.Dwh.Exec(sqlQuery)
			if ColumnAlreadyExistErr(err, args.Dwh.Label()) {
				err = nil
			} else if err != nil {
				return err
			}
		}
	}

	if err == nil {
		// createTable = false since it all successfully updated.
		args.Tc.MutateInMemoryColumns(false, args.ColumnOp, mutateCol...)
	}

	return nil
}
