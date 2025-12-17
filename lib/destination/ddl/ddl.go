package ddl

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func shouldCreatePrimaryKey(col columns.Column, mode config.Mode, createTable bool) bool {
	return col.PrimaryKey() && mode == config.Replication && createTable
}

func BuildCreateTableSQL(settings config.SharedDestinationColumnSettings, dialect sql.Dialect, tableIdentifier sql.TableIdentifier, temporaryTable bool, mode config.Mode, columns []columns.Column) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("no columns provided")
	}

	var parts []string
	var primaryKeys []string
	for _, col := range columns {
		if col.ShouldSkip() {
			// It should be filtered upstream
			return "", fmt.Errorf("received an invalid column %q", col.Name())
		}

		colName := dialect.QuoteIdentifier(col.Name())
		if shouldCreatePrimaryKey(col, mode, true) {
			primaryKeys = append(primaryKeys, colName)
		}

		dataType, err := dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey(), settings)
		if err != nil {
			return "", fmt.Errorf("failed to get data type for column %q: %w", col.Name(), err)
		}

		parts = append(parts, fmt.Sprintf("%s %s", colName, dataType))
	}

	if len(primaryKeys) > 0 {
		pkStatement := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		if _, ok := dialect.(bigQueryDialect.BigQueryDialect); ok {
			pkStatement += " NOT ENFORCED"
		}

		parts = append(parts, pkStatement)
	}

	return dialect.BuildCreateTableQuery(tableIdentifier, temporaryTable, mode, parts), nil
}

// DropTemporaryTable - this will drop the temporary table from Snowflake w/ stages and BigQuery
// It has a safety check to make sure the tableName contains the `constants.ArtiePrefix` key.
// Temporary tables look like this: database.schema.tableName__artie__RANDOM_STRING(5)_expiryUnixTs
func DropTemporaryTable(ctx context.Context, dest destination.Destination, tableIdentifier sql.TableIdentifier, shouldReturnError bool) error {
	if strings.Contains(strings.ToLower(tableIdentifier.Table()), constants.ArtiePrefix) {
		sqlCommand := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdentifier.FullyQualifiedName())
		if _, err := dest.ExecContext(ctx, sqlCommand); err != nil {
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

func BuildAlterTableAddColumns(settings config.SharedDestinationColumnSettings, dialect sql.Dialect, tableID sql.TableIdentifier, cols []columns.Column) ([]string, error) {
	var parts []string
	for _, col := range cols {
		if col.ShouldSkip() {
			return nil, fmt.Errorf("received an invalid column %q", col.Name())
		}

		dataType, err := dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey(), settings)
		if err != nil {
			return nil, fmt.Errorf("failed to get data type for column %q: %w", col.Name(), err)
		}

		sqlPart := fmt.Sprintf("%s %s", dialect.QuoteIdentifier(col.Name()), dataType)
		parts = append(parts, dialect.BuildAddColumnQuery(tableID, sqlPart))
	}

	return parts, nil
}

func BuildAlterTableDropColumns(dialect sql.Dialect, tableID sql.TableIdentifier, col columns.Column) (string, error) {
	if col.ShouldSkip() {
		return "", fmt.Errorf("received an invalid column %q", col.Name())
	}

	return dialect.BuildDropColumnQuery(tableID, dialect.QuoteIdentifier(col.Name())), nil
}
