package ddl

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
)

// DropTemporaryTable - this will drop the temporary table from Snowflake w/ stages and BigQuery
// It has a safety check to make sure the tableName contains the `constants.ArtiePrefix` key.
// Temporary tables look like this: database.schema.tableName__artie__RANDOM_STRING(5)_expiryUnixTs
func DropTemporaryTable(dwh destination.DataWarehouse, fqTableName string, shouldReturnError bool) error {
	if strings.Contains(strings.ToLower(fqTableName), constants.ArtiePrefix) {
		_, err := dwh.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", fqTableName))
		if err != nil {
			slog.Warn("Failed to drop temporary table, it will get garbage collected by the TTL...", slog.Any("err", err))
			if shouldReturnError {
				return fmt.Errorf("failed to drop temp table: %w", err)
			}
		}
	} else {
		slog.Warn(fmt.Sprintf("Skipped dropping table: %s because it does not contain the artie prefix", fqTableName))
	}

	return nil
}

type AlterTableArgs struct {
	Dwh destination.DataWarehouse
	Tc  *types.DwhTableConfig
	// ContainsOtherOperations - this is sourced from tableData `containOtherOperations`
	ContainOtherOperations bool
	FqTableName            string
	CreateTable            bool
	TemporaryTable         bool
	UppercaseEscNames      *bool

	ColumnOp constants.ColumnOperation
	Mode     config.Mode

	CdcTime time.Time
}

func (a *AlterTableArgs) Validate() error {
	// You can't DROP a column and try to create a table at the same time.
	if a.ColumnOp == constants.Delete && a.CreateTable {
		return fmt.Errorf("incompatible operation - cannot drop columns and create table at the same time")
	}

	if !(a.Mode == config.History || a.Mode == config.Replication) {
		return fmt.Errorf("unexpected mode: %s", a.Mode.String())
	}

	// Temporary tables should only be created, not altered.
	if a.TemporaryTable {
		if !a.CreateTable {
			return fmt.Errorf("incompatible operation - we should not be altering temporary tables, only create")
		}
	}

	if a.UppercaseEscNames == nil {
		return fmt.Errorf("uppercaseEscNames cannot be nil")
	}

	return nil
}

func AlterTable(args AlterTableArgs, cols ...columns.Column) error {
	if err := args.Validate(); err != nil {
		return err
	}

	if len(cols) == 0 {
		return nil
	}

	var mutateCol []columns.Column
	// It's okay to combine since args.ColumnOp only takes one of: `Delete` or `Add`
	var colSQLParts []string
	var pkCols []string
	for _, col := range cols {
		if col.ShouldSkip() {
			// Let's not modify the table if the column kind is invalid
			continue
		}

		if args.ColumnOp == constants.Delete {
			if !args.Tc.ShouldDeleteColumn(col.RawName(), args.CdcTime, args.ContainOtherOperations) {
				continue
			}
		}

		mutateCol = append(mutateCol, col)
		switch args.ColumnOp {
		case constants.Add:
			colName := col.Name(*args.UppercaseEscNames, &sql.NameArgs{
				Escape:   true,
				DestKind: args.Dwh.Label(),
			})

			if col.PrimaryKey() && args.Mode != config.History {
				// Don't create a PK for history mode because it's append-only, so the primary key should not be enforced.
				pkCols = append(pkCols, colName)
			}

			colSQLParts = append(colSQLParts, fmt.Sprintf(`%s %s`, colName, typing.KindToDWHType(col.KindDetails, args.Dwh.Label(), col.PrimaryKey())))
		case constants.Delete:
			colSQLParts = append(colSQLParts, col.Name(*args.UppercaseEscNames, &sql.NameArgs{
				Escape:   true,
				DestKind: args.Dwh.Label(),
			}))
		}
	}

	if len(pkCols) > 0 {
		pkStatement := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", "))
		if args.Dwh.Label() == constants.BigQuery {
			pkStatement += " NOT ENFORCED"
		}

		colSQLParts = append(colSQLParts, pkStatement)
	}

	var err error
	if args.CreateTable {
		var sqlQuery string
		if args.TemporaryTable {
			switch args.Dwh.Label() {
			case constants.MSSQL:
				sqlQuery = fmt.Sprintf("CREATE TABLE %s (%s);", args.FqTableName, strings.Join(colSQLParts, ","))
			case constants.Redshift:
				sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", args.FqTableName, strings.Join(colSQLParts, ","))
			case constants.BigQuery:
				sqlQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) OPTIONS (expiration_timestamp = TIMESTAMP("%s"))`,
					args.FqTableName, strings.Join(colSQLParts, ","), typing.ExpiresDate(time.Now().UTC().Add(constants.TemporaryTableTTL)))
			// Not enabled for constants.Snowflake yet
			case constants.Snowflake:
				// TEMPORARY Table syntax - https://docs.snowflake.com/en/sql-reference/sql/create-table
				// PURGE syntax - https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#purging-files-after-loading
				// FIELD_OPTIONALLY_ENCLOSED_BY - is needed because CSV will try to escape any values that have `"`
				sqlQuery = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) STAGE_COPY_OPTIONS = ( PURGE = TRUE ) STAGE_FILE_FORMAT = ( TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE)`,
					args.FqTableName, strings.Join(colSQLParts, ","))
			default:
				return fmt.Errorf("unexpected dwh: %v trying to create a temporary table", args.Dwh.Label())
			}
		} else {
			if args.Dwh.Label() == constants.MSSQL {
				// MSSQL doesn't support IF NOT EXISTS
				sqlQuery = fmt.Sprintf("CREATE TABLE %s (%s)", args.FqTableName, strings.Join(colSQLParts, ","))
			} else {
				sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", args.FqTableName, strings.Join(colSQLParts, ","))
			}
		}

		slog.Info("DDL - executing sql", slog.String("query", sqlQuery))
		_, err = args.Dwh.Exec(sqlQuery)
		if ColumnAlreadyExistErr(err, args.Dwh.Label()) {
			err = nil
		} else if err != nil {
			return err
		}
	} else {
		for _, colSQLPart := range colSQLParts {
			var sqlQuery string
			if args.Dwh.Label() == constants.MSSQL {
				// MSSQL doesn't support the COLUMN keyword
				sqlQuery = fmt.Sprintf("ALTER TABLE %s %s %s", args.FqTableName, args.ColumnOp, colSQLPart)
			} else {
				sqlQuery = fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", args.FqTableName, args.ColumnOp, colSQLPart)
			}

			slog.Info("DDL - executing sql", slog.String("query", sqlQuery))
			_, err = args.Dwh.Exec(sqlQuery)
			if ColumnAlreadyExistErr(err, args.Dwh.Label()) {
				err = nil
			} else if err != nil {
				return fmt.Errorf("failed to apply ddl, sql: %v, err: %w", sqlQuery, err)
			}
		}
	}

	if err == nil {
		// createTable = false since it all successfully updated.
		args.Tc.MutateInMemoryColumns(false, args.ColumnOp, mutateCol...)
	}

	return nil
}
