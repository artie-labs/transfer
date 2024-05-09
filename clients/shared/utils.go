package shared

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BackfillColumn(dwh destination.DataWarehouse, column columns.Column, tableID types.TableIdentifier) error {
	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	if _, ok := dwh.Dialect().(sql.MSSQLDialect); ok {
		// TODO: Support MSSQL column backfill
		return nil
	}

	defaultVal, err := column.DefaultValue(dwh.Dialect(), dwh.AdditionalDateFormats())
	if err != nil {
		return fmt.Errorf("failed to escape default value: %w", err)
	}

	escapedCol := dwh.Dialect().QuoteIdentifier(column.Name())

	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		tableID.FullyQualifiedName(), escapedCol, defaultVal, escapedCol,
	)
	slog.Info("Backfilling column",
		slog.String("colName", column.Name()),
		slog.String("query", query),
		slog.String("table", tableID.FullyQualifiedName()),
	)

	_, err = dwh.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to backfill, err: %w, query: %v", err, query)
	}

	query = fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%v';`, tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`)
	if dwh.Label() == constants.BigQuery {
		query = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (description=`%s`);",
			// ALTER TABLE table ALTER COLUMN col set OPTIONS (description=...)
			tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`,
		)
	}

	_, err = dwh.Exec(query)
	return err
}
