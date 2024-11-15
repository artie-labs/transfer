package shared

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func CreateTable(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, tc *types.DwhTableConfig, tableID sql.TableIdentifier, tempTable bool) error {
	query, err := ddl.BuildCreateTableSQL(dwh.Dialect(), tableID, tempTable, tableData.Mode(), tableData.ReadOnlyInMemoryCols().GetColumns())
	if err != nil {
		return fmt.Errorf("failed to build create table sql: %w", err)
	}

	slog.Info("[DDL] Executing query", slog.String("query", query))
	if _, err = dwh.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Update cache with the new columns that we've added.
	tc.MutateInMemoryColumns(constants.Add, tableData.ReadOnlyInMemoryCols().GetColumns()...)
	return nil
}

func AlterTableAddColumns(ctx context.Context, dwh destination.DataWarehouse, tc *types.DwhTableConfig, tableID sql.TableIdentifier, columns []columns.Column) error {
	if len(columns) == 0 {
		return nil
	}

	sqlParts, addedCols := ddl.BuildAlterTableAddColumns(dwh.Dialect(), tableID, columns)
	for _, sqlPart := range sqlParts {
		slog.Info("[DDL] Executing query", slog.String("query", sqlPart))
		if _, err := dwh.ExecContext(ctx, sqlPart); err != nil {
			if !dwh.Dialect().IsColumnAlreadyExistsErr(err) {
				return fmt.Errorf("failed to alter table: %w", err)
			}
		}
	}

	tc.MutateInMemoryColumns(constants.Add, addedCols...)
	return nil
}
