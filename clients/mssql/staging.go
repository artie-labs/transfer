package mssql

import (
	"context"
	"fmt"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, _ *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		query, err := ddl.BuildCreateTableSQL(s.Dialect(), tempTableID, true, tableData.Mode(), tableData.ReadOnlyInMemoryCols().GetColumns())
		if err != nil {
			return fmt.Errorf("failed to build create table sql: %w", err)
		}

		if _, err = s.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

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

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	stmt, err := tx.Prepare(mssql.CopyIn(tempTableID.FullyQualifiedName(), mssql.BulkOptions{}, columns.ColumnNames(cols)...))
	if err != nil {
		return fmt.Errorf("failed to prepare bulk insert: %w", err)
	}

	defer stmt.Close()

	for _, value := range tableData.Rows() {
		var row []any
		for _, col := range cols {
			castedValue, castErr := parseValue(value[col.Name()], col)
			if castErr != nil {
				return castErr
			}

			row = append(row, castedValue)
		}

		if _, err = stmt.Exec(row...); err != nil {
			return fmt.Errorf("failed to copy row: %w", err)
		}
	}

	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("failed to finalize bulk insert: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	txCommitted = true
	return nil
}
