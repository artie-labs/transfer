package ddl

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BuildCreateTableSQL(dialect sql.Dialect, tableIdentifier sql.TableIdentifier, temporaryTable bool, mode config.Mode, columns []columns.Column) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("no columns provided")
	}

	var parts []string
	var primaryKeys []string
	for _, col := range columns {
		if col.ShouldSkip() {
			continue
		}

		colName := dialect.QuoteIdentifier(col.Name())
		if shouldCreatePrimaryKey(col, mode, true) {
			primaryKeys = append(primaryKeys, colName)
		}

		parts = append(parts, fmt.Sprintf("%s %s", colName, dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey())))
	}

	if len(primaryKeys) > 0 {
		pkStatement := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		if _, ok := dialect.(bigQueryDialect.BigQueryDialect); ok {
			pkStatement += " NOT ENFORCED"
		}

		parts = append(parts, pkStatement)
	}

	return dialect.BuildCreateTableQuery(tableIdentifier, temporaryTable, parts), nil
}

// DropTemporaryTable - this will drop the temporary table from Snowflake w/ stages and BigQuery
// It has a safety check to make sure the tableName contains the `constants.ArtiePrefix` key.
// Temporary tables look like this: database.schema.tableName__artie__RANDOM_STRING(5)_expiryUnixTs
func DropTemporaryTable(dwh destination.DataWarehouse, tableIdentifier sql.TableIdentifier, shouldReturnError bool) error {
	if strings.Contains(strings.ToLower(tableIdentifier.Table()), constants.ArtiePrefix) {
		sqlCommand := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdentifier.FullyQualifiedName())
		if _, err := dwh.Exec(sqlCommand); err != nil {
			slog.Warn("Failed to drop temporary table, it will get garbage collected by the TTL...",
				slog.Any("err", err),
				slog.String("sqlCommand", sqlCommand),
			)
			if shouldReturnError {
				return fmt.Errorf("failed to drop temp table: %w", err)
			}
		}
	} else {
		slog.Warn(fmt.Sprintf("Skipped dropping table: %s because it does not contain the artie prefix", tableIdentifier.FullyQualifiedName()))
	}

	return nil
}

func BuildAlterTableAddColumns(dialect sql.Dialect, tableID sql.TableIdentifier, cols []columns.Column) ([]string, []columns.Column) {
	var parts []string
	var addedCols []columns.Column
	for _, col := range cols {
		if col.ShouldSkip() {
			continue
		}

		sqlPart := fmt.Sprintf("%s %s", dialect.QuoteIdentifier(col.Name()), dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey()))
		parts = append(parts, dialect.BuildAlterColumnQuery(tableID, constants.Add, sqlPart))
		addedCols = append(addedCols, col)
	}

	return parts, addedCols
}

type AlterTableArgs struct {
	Dialect sql.Dialect
	Tc      *types.DwhTableConfig
	// ContainsOtherOperations - this is sourced from tableData `containOtherOperations`
	ContainOtherOperations bool
	TableID                sql.TableIdentifier
	ColumnOp               constants.ColumnOperation
	Mode                   config.Mode
	CdcTime                time.Time
}

func (a AlterTableArgs) Validate() error {
	if a.Dialect == nil {
		return fmt.Errorf("dialect cannot be nil")
	}

	if !(a.Mode == config.History || a.Mode == config.Replication) {
		return fmt.Errorf("unexpected mode: %s", a.Mode.String())
	}

	return nil
}

func shouldCreatePrimaryKey(col columns.Column, mode config.Mode, createTable bool) bool {
	return col.PrimaryKey() && mode == config.Replication && createTable
}

func (a AlterTableArgs) buildStatements(cols ...columns.Column) ([]string, []columns.Column) {
	var mutateCol []columns.Column
	// It's okay to combine since args.ColumnOp only takes one of: `Delete` or `Add`
	var colSQLParts []string
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
			colSQLParts = append(colSQLParts, fmt.Sprintf("%s %s", a.Dialect.QuoteIdentifier(col.Name()), a.Dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey())))
		case constants.Delete:
			colSQLParts = append(colSQLParts, a.Dialect.QuoteIdentifier(col.Name()))
		}
	}

	var alterStatements []string
	for _, colSQLPart := range colSQLParts {
		alterStatements = append(alterStatements, a.Dialect.BuildAlterColumnQuery(a.TableID, a.ColumnOp, colSQLPart))
	}

	return alterStatements, mutateCol
}

func (a AlterTableArgs) AlterTable(dwh destination.DataWarehouse, cols ...columns.Column) error {
	if err := a.Validate(); err != nil {
		return err
	}

	if len(cols) == 0 {
		return nil
	}

	alterStatements, mutateCol := a.buildStatements(cols...)
	for _, sqlQuery := range alterStatements {
		slog.Info("DDL - executing sql", slog.String("query", sqlQuery))
		if _, err := dwh.Exec(sqlQuery); err != nil {
			if !a.Dialect.IsColumnAlreadyExistsErr(err) {
				return fmt.Errorf("failed to apply ddl, sql: %q, err: %w", sqlQuery, err)
			}
		}
	}

	// createTable = false since it all successfully updated.
	a.Tc.MutateInMemoryColumns(a.ColumnOp, mutateCol...)

	return nil
}
