package shared

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func getValidColumns(cols []columns.Column) []columns.Column {
	var validCols []columns.Column
	for _, col := range cols {
		if col.ShouldSkip() {
			continue
		}

		validCols = append(validCols, col)
	}

	return validCols
}

func CreateTempTable(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, tc *types.DwhTableConfig, settings config.SharedDestinationColumnSettings, tableID sql.TableIdentifier) error {
	return CreateTable(ctx, dwh, tableData.Mode(), tc, settings, tableID, true, tableData.ReadOnlyInMemoryCols().GetColumns())
}

func CreateTable(ctx context.Context, dwh destination.DataWarehouse, mode config.Mode, tc *types.DwhTableConfig, settings config.SharedDestinationColumnSettings, tableID sql.TableIdentifier, tempTable bool, cols []columns.Column) error {
	cols = getValidColumns(cols)
	if len(cols) == 0 {
		return nil
	}

	query, err := ddl.BuildCreateTableSQL(settings, dwh.Dialect(), tableID, tempTable, mode, cols)
	if err != nil {
		return fmt.Errorf("failed to build create table sql: %w", err)
	}

	slog.Info("[DDL] Executing query", slog.String("query", query))
	if _, err = dwh.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Update cache with the new columns that we've added.
	tc.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func AlterTableAddColumns(ctx context.Context, dwh destination.DataWarehouse, tc *types.DwhTableConfig, settings config.SharedDestinationColumnSettings, tableID sql.TableIdentifier, cols []columns.Column) error {
	cols = getValidColumns(cols)
	if len(cols) == 0 {
		return nil
	}

	sqlParts, err := ddl.BuildAlterTableAddColumns(settings, dwh.Dialect(), tableID, cols)
	if err != nil {
		return fmt.Errorf("failed to build alter table add columns: %w", err)
	}

	for _, sqlPart := range sqlParts {
		slog.Info("[DDL] Executing query", slog.String("query", sqlPart))
		if _, err = dwh.ExecContext(ctx, sqlPart); err != nil {
			if !dwh.Dialect().IsColumnAlreadyExistsErr(err) {
				return fmt.Errorf("failed to alter table: %w", err)
			}
		}
	}

	tc.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func AlterTableDropColumns(ctx context.Context, dwh destination.DataWarehouse, tc *types.DwhTableConfig, tableID sql.TableIdentifier, cols []columns.Column, cdcTime time.Time, containOtherOperations bool) error {
	if len(cols) == 0 {
		return nil
	}

	var colsToDrop []columns.Column
	for _, col := range cols {
		if tc.ShouldDeleteColumn(col.Name(), cdcTime, containOtherOperations) {
			colsToDrop = append(colsToDrop, col)
		}
	}

	if len(colsToDrop) == 0 {
		return nil
	}

	for _, colToDrop := range colsToDrop {
		query, err := ddl.BuildAlterTableDropColumns(dwh.Dialect(), tableID, colToDrop)
		if err != nil {
			return fmt.Errorf("failed to build alter table drop columns: %w", err)
		}

		slog.Info("[DDL] Executing query", slog.String("query", query))
		if _, err = dwh.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	tc.MutateInMemoryColumns(constants.Delete, colsToDrop...)
	return nil
}
