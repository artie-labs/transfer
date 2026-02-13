package mssql

import (
	"context"
	goSql "database/sql"
	"fmt"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

	return db.CommitOrRollback(tx, func(tx *goSql.Tx) error {
		cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
		stmt, err := tx.Prepare(mssql.CopyIn(tableID.FullyQualifiedName(), mssql.BulkOptions{}, columns.ColumnNames(cols)...))
		if err != nil {
			return fmt.Errorf("failed to prepare bulk insert: %w", err)
		}

		defer stmt.Close()

		for _, row := range tableData.Rows() {
			var parsedValues []any
			for _, col := range cols {
				value, _ := row.GetValue(col.Name())
				parsedValue, err := shared.ParseValue(value, col)
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

		return nil
	})
}
