package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func Append(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.AdditionalSettings) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tableConfig, err := dwh.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.Diff(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	if tableConfig.CreateTable() {
		if err = CreateTable(ctx, dwh, tableData, tableConfig, opts.ColumnSettings, tableID, false, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err = AlterTableAddColumns(ctx, dwh, tableConfig, opts.ColumnSettings, tableID, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	tempTableID := tableID
	if opts.UseTempTable {
		// Override tableID with tempTableID if we're using a temporary table
		tempTableID = opts.TempTableID
	}

	return dwh.PrepareTemporaryTable(
		ctx,
		tableData,
		tableConfig,
		tempTableID,
		tableID,
		opts,
		opts.UseTempTable,
	)
}
