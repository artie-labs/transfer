package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func ExecuteMergeOperations(ctx context.Context, dest destination.Destination, tableData *optimization.TableData, tableID sql.TableIdentifier, subQuery string, opts types.MergeOpts) error {
	if subQuery == "" {
		return fmt.Errorf("subQuery cannot be empty")
	}

	cols := tableData.ReadOnlyInMemoryCols()

	var primaryKeys []columns.Column
	for _, primaryKey := range tableData.PrimaryKeys() {
		column, ok := cols.GetColumn(primaryKey)
		if !ok {
			return fmt.Errorf("column for primary key %q does not exist", primaryKey)
		}
		primaryKeys = append(primaryKeys, column)
	}

	if len(primaryKeys) == 0 {
		return fmt.Errorf("primary keys cannot be empty")
	}

	validColumns := cols.ValidColumns()
	if len(validColumns) == 0 {
		return fmt.Errorf("columns cannot be empty")
	}
	for _, column := range validColumns {
		if column.ShouldSkip() {
			return fmt.Errorf("column %q is invalid and should be skipped", column.Name())
		}
	}

	mergeStatements, err := dest.Dialect().BuildMergeQueries(
		tableID,
		subQuery,
		primaryKeys,
		opts.AdditionalEqualityStrings,
		validColumns,
		tableData.TopicConfig().SoftDelete,
		tableData.ContainsHardDeletes(),
	)
	if err != nil {
		return fmt.Errorf("failed to generate merge statements: %w", err)
	}

	results, err := destination.ExecContextStatements(ctx, dest, mergeStatements)
	if err != nil {
		return fmt.Errorf("failed to execute merge statements: %w", err)
	}

	if dest.GetConfig().SharedDestinationSettings.EnableMergeAssertion {
		var totalRowsAffected int64
		for _, result := range results {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			}
			totalRowsAffected += rowsAffected
		}

		if rows := tableData.NumberOfRows(); rows > uint(totalRowsAffected) {
			return fmt.Errorf("expected %d rows to be affected, got %d", rows, totalRowsAffected)
		}
	}

	return nil
}
