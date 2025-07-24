package postgres

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/jackc/pgx/v5"
)

func (s *Store) buildPgxConn(ctx context.Context) (*pgx.Conn, func(), error) {

}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	conn, err := s.Store.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get pgx conn: %w", err)
	}

	defer conn.Close()

	conn.Raw(func(driverConn any) error {
		pgxConn, ok := driverConn.(*pgx.Conn)
		if !ok {
			return fmt.Errorf("expected pgx.Conn, got %T", driverConn)
		}

		cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
		copyCount, err := pgxConn.CopyFrom(ctx, pgx.Identifier{tempTableID.FullyQualifiedName()}, columns.ColumnNames(cols), pgx.CopyFromSlice(len(tableData.Rows()), func(i int) ([]any, error) {
			return tableData.Rows()[i], nil
		}))
		if err != nil {
			return fmt.Errorf("failed to copy from rows: %w", err)
		}

		if copyCount != int64(tableData.NumberOfRows()) {
			return fmt.Errorf("expected %d rows to be copied, but got %d", tableData.NumberOfRows(), copyCount)
		}

		return nil
	})

	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	var txCommitted bool
	defer func() {
		if !txCommitted {
			tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(mssql.CopyIn(tempTableID.FullyQualifiedName(), mssql.BulkOptions{}, columns.ColumnNames(cols)...))
	if err != nil {
		return fmt.Errorf("failed to prepare bulk insert: %w", err)
	}

	defer stmt.Close()

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

		if _, err = stmt.ExecContext(ctx, parsedValues...); err != nil {
			return fmt.Errorf("failed to copy row: %w", err)
		}
	}

	results, err := stmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to finalize bulk insert: %w", err)
	}

	rowsLoaded, err := results.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
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

func parseValue(value any, col columns.Column) (any, error) {
	return value, fmt.Errorf("not implemented")
}
