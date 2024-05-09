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
)

// DropTemporaryTable - this will drop the temporary table from Snowflake w/ stages and BigQuery
// It has a safety check to make sure the tableName contains the `constants.ArtiePrefix` key.
// Temporary tables look like this: database.schema.tableName__artie__RANDOM_STRING(5)_expiryUnixTs
func DropTemporaryTable(dwh destination.DataWarehouse, fqTableName string, shouldReturnError bool) error {
	if strings.Contains(strings.ToLower(fqTableName), constants.ArtiePrefix) {
		sqlCommand := fmt.Sprintf("DROP TABLE IF EXISTS %s", fqTableName)
		slog.Debug("Dropping temporary table", slog.String("sql", sqlCommand))
		if _, err := dwh.Exec(sqlCommand); err != nil {
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
	TableID                types.TableIdentifier
	CreateTable            bool
	TemporaryTable         bool

	ColumnOp constants.ColumnOperation
	Mode     config.Mode

	CdcTime time.Time
}

func (a AlterTableArgs) Validate() error {
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

	return nil
}

func (a AlterTableArgs) buildStatements(cols ...columns.Column) ([]string, []columns.Column) {
	var mutateCol []columns.Column
	// It's okay to combine since args.ColumnOp only takes one of: `Delete` or `Add`
	var colSQLParts []string
	var pkCols []string
	for _, col := range cols {
		if col.ShouldSkip() {
			// Let's not modify the table if the column kind is invalid
			continue
		}

		if a.ColumnOp == constants.Delete {
			if !a.Tc.ShouldDeleteColumn(col.Name(), a.CdcTime, a.ContainOtherOperations) {
				continue
			}
		}

		mutateCol = append(mutateCol, col)
		switch a.ColumnOp {
		case constants.Add:
			colName := a.Dwh.Dialect().QuoteIdentifier(col.Name())

			if col.PrimaryKey() && a.Mode != config.History {
				// Don't create a PK for history mode because it's append-only, so the primary key should not be enforced.
				pkCols = append(pkCols, colName)
			}

			colSQLParts = append(colSQLParts, fmt.Sprintf(`%s %s`, colName, a.Dwh.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey())))
		case constants.Delete:
			colSQLParts = append(colSQLParts, a.Dwh.Dialect().QuoteIdentifier(col.Name()))
		}
	}

	if len(pkCols) > 0 {
		pkStatement := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", "))
		if _, ok := a.Dwh.Dialect().(sql.BigQueryDialect); ok {
			pkStatement += " NOT ENFORCED"
		}

		colSQLParts = append(colSQLParts, pkStatement)
	}

	fqTableName := a.TableID.FullyQualifiedName()

	var alterStatements []string
	if a.CreateTable {
		var sqlQuery string
		if a.TemporaryTable {
			sqlQuery = a.Dwh.Dialect().BuildCreateTempTableQuery(fqTableName, colSQLParts)
		} else {
			if _, ok := a.Dwh.Dialect().(sql.MSSQLDialect); ok {
				// MSSQL doesn't support IF NOT EXISTS
				sqlQuery = fmt.Sprintf("CREATE TABLE %s (%s)", fqTableName, strings.Join(colSQLParts, ","))
			} else {
				sqlQuery = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fqTableName, strings.Join(colSQLParts, ","))
			}
		}

		alterStatements = []string{sqlQuery}
	} else {
		for _, colSQLPart := range colSQLParts {
			var sqlQuery string
			if _, ok := a.Dwh.Dialect().(sql.MSSQLDialect); ok {
				// MSSQL doesn't support the COLUMN keyword
				sqlQuery = fmt.Sprintf("ALTER TABLE %s %s %s", fqTableName, a.ColumnOp, colSQLPart)
			} else {
				sqlQuery = fmt.Sprintf("ALTER TABLE %s %s COLUMN %s", fqTableName, a.ColumnOp, colSQLPart)
			}
			alterStatements = append(alterStatements, sqlQuery)
		}
	}

	return alterStatements, mutateCol
}

func (a AlterTableArgs) AlterTable(cols ...columns.Column) error {
	if err := a.Validate(); err != nil {
		return err
	}

	if len(cols) == 0 {
		return nil
	}

	alterStatements, mutateCol := a.buildStatements(cols...)

	for _, sqlQuery := range alterStatements {
		slog.Info("DDL - executing sql", slog.String("query", sqlQuery))
		if _, err := a.Dwh.Exec(sqlQuery); err != nil {
			if a.Dwh.Dialect().IsColumnAlreadyExistsErr(err) {
				err = nil
			} else {
				return fmt.Errorf("failed to apply ddl, sql: %q, err: %w", sqlQuery, err)
			}
		}
	}

	// createTable = false since it all successfully updated.
	a.Tc.MutateInMemoryColumns(false, a.ColumnOp, mutateCol...)

	return nil
}
