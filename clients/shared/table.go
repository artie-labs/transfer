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
)

func CreateTable(_ context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, tc *types.DwhTableConfig, tableID sql.TableIdentifier, tempTable bool) error {
	query, err := ddl.BuildCreateTableSQL(dwh.Dialect(), tableID, tempTable, tableData.Mode(), tableData.ReadOnlyInMemoryCols().GetColumns())
	if err != nil {
		return fmt.Errorf("failed to build create table sql: %w", err)
	}

	slog.Info("[DDL] Executing query", slog.String("query", query))
	// TODO: Use ExecContext
	if _, err = dwh.Exec(query); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Update cache with the new columns that we've added.
	tc.MutateInMemoryColumns(true, constants.Add, tableData.ReadOnlyInMemoryCols().GetColumns()...)
	return nil
}
