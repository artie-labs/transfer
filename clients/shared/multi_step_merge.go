package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func MultiStepMerge(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.MergeOpts) error {
	// 1. We should load the table config based on the destination table to grab the column definitions
	// 2. The very first time we create this table, we sohuld just simply load the intermittent staging table
	// 3. Afterwards, we'll create a staging table and merge that into the intermittent staging table
	// 4. Then we'll drop the staging table
	// 5. Upon the last attempt, we'll merge the staging table into the intermittent staging table
	// 6. Then merge the intermittent staging table into the destination table
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	_, targetKeysMissing := columns.Diff(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	if tableConfig.CreateTable() {
		// The intermittent table should be treated as a temp table as well.
		if err = CreateTable(ctx, dwh, tableData, tableConfig, opts.ColumnSettings, tableID, true, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	tableData.SetFlushCountRemaining(tableData.GetFlushCountRemaining() - 1)

	if tableData.GetFlushCountRemaining() == 0 {
		// We've reached the last step, so we should merge the staging table into the destination table
		if err = Merge(ctx, dwh, tableData, opts); err != nil {
			return fmt.Errorf("failed to merge: %w", err)
		}
	}

}
