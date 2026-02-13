package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	libsql "github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// batchSize is the number of rows to insert per batch INSERT statement.
// MySQL has a max_allowed_packet limit (default 64MB), so we use a conservative batch size.
const batchSize = 1000

func (s *Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ libsql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return err
		}
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	rows := tableData.Rows()

	if len(rows) == 0 {
		return nil
	}

	// Begin transaction for batch inserts
	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	return db.CommitOrRollback(tx, func(tx *sql.Tx) error {
		var rowsLoaded int64
		for batch := range slices.Chunk(rows, batchSize) {
			affected, err := s.executeBatchInsert(ctx, tx, tableID, cols, batch)
			if err != nil {
				return fmt.Errorf("failed to execute batch insert: %w", err)
			}

			rowsLoaded += affected
		}

		if expectedRows := int64(tableData.NumberOfRows()); rowsLoaded != expectedRows {
			return fmt.Errorf("expected %d rows to be loaded, but got %d", expectedRows, rowsLoaded)
		}

		return nil
	})
}

// executeBatchInsert executes a batch INSERT statement for the given rows.
// It builds a query like: INSERT INTO table (col1, col2) VALUES (?, ?), (?, ?), ...
func (s *Store) executeBatchInsert(ctx context.Context, tx *sql.Tx, tableID libsql.TableIdentifier, cols []columns.Column, rows []optimization.Row) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns to insert")
	}

	// Build column names
	var colNames []string
	for _, col := range cols {
		colNames = append(colNames, s.dialect().QuoteIdentifier(col.Name()))
	}

	// Build placeholders for one row: (?, ?, ?)
	singleRowPlaceholder := "(" + strings.Repeat("?, ", len(cols)-1) + "?)"

	// Build placeholders for all rows: (?, ?), (?, ?), ...
	allPlaceholders := make([]string, len(rows))
	for i := range rows {
		allPlaceholders[i] = singleRowPlaceholder
	}

	// Build the INSERT query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableID.FullyQualifiedName(),
		strings.Join(colNames, ", "),
		strings.Join(allPlaceholders, ", "),
	)

	// Collect all values for the batch
	var values []any
	for _, row := range rows {
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			parsedValue, err := shared.ParseValue(value, col)
			if err != nil {
				return 0, fmt.Errorf("failed to parse value for column %q: %w", col.Name(), err)
			}

			values = append(values, parsedValue)
		}
	}

	result, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute batch insert: %w", err)
	}

	return result.RowsAffected()
}
