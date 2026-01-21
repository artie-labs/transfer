package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

func (s *Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return err
		}
	}

	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				slog.Warn("failed to rollback transaction", slog.Any("error", rollbackErr))
			}
		}
	}()

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()

	// Build the INSERT statement with placeholders
	colNames := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = s.dialect().QuoteIdentifier(col.Name())
		placeholders[i] = "?"
	}

	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableID.FullyQualifiedName(),
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	var rowsLoaded int64
	for _, row := range tableData.Rows() {
		var parsedValues []any
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			parsedValue, err := parseValue(value, col)
			if err != nil {
				return fmt.Errorf("failed to parse value: %w", err)
			}

			parsedValues = append(parsedValues, parsedValue)
		}

		result, err := stmt.ExecContext(ctx, parsedValues...)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		rowsLoaded += affected
	}

	if expectedRows := int64(tableData.NumberOfRows()); rowsLoaded != expectedRows {
		return fmt.Errorf("expected %d rows to be loaded, but got %d", expectedRows, rowsLoaded)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	txCommitted = true
	return nil
}
