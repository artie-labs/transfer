package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func MultiStepMerge(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.MergeOpts) error {
	if _, ok := dwh.Dialect().(dialect.SnowflakeDialect); !ok {
		return fmt.Errorf("multi-step merge is only supported on Snowflake")
	}

	msmSettings := tableData.MultiStepMergeSettings()
	if !msmSettings.Enabled {
		return fmt.Errorf("multi-step merge is not enabled")
	}

	msmTableID := dwh.IdentifierFor(tableData.TopicConfig(), fmt.Sprintf("%s_msm", tableData.Name()))

	targetTableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	targetTableConfig, err := dwh.GetTableConfig(targetTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	if msmSettings.FlushCount == 0 {
		// If it's the first time we are doing this, we should ensure the MSM table has been dropped.
		if err := dwh.DropTable(ctx, msmTableID); err != nil {
			return fmt.Errorf("failed to drop msm table: %w", err)
		}

		// We should now align our columns against the target table.
		if err = tableData.MergeColumnsFromDestination(targetTableConfig.GetColumns()...); err != nil {
			return fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
		}
	}

	msmTableConfig, err := dwh.GetTableConfig(msmTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	{
		// Apply schema evolution for the MSM table
		// We're not going to drop columns for MSM yet.
		_, targetKeysMissing := columns.DiffAndFilter(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			msmTableConfig.GetColumns(),
			tableData.TopicConfig().SoftDelete,
			tableData.TopicConfig().IncludeArtieUpdatedAt,
			tableData.TopicConfig().IncludeDatabaseUpdatedAt,
			tableData.Mode(),
		)

		if msmTableConfig.CreateTable() {
			if err = CreateTable(ctx, dwh, tableData.Mode(), msmTableConfig, opts.ColumnSettings, msmTableID, false, targetKeysMissing); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dwh, msmTableConfig, opts.ColumnSettings, msmTableID, targetKeysMissing); err != nil {
				return fmt.Errorf("failed to add columns for table %q: %w", msmTableID.Table(), err)
			}
		}
	}
	{
		// Apply schema evolution for the target table
		_, targetKeysMissing := columns.DiffAndFilter(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			targetTableConfig.GetColumns(),
			tableData.TopicConfig().SoftDelete,
			tableData.TopicConfig().IncludeArtieUpdatedAt,
			tableData.TopicConfig().IncludeDatabaseUpdatedAt,
			tableData.Mode(),
		)

		if targetTableConfig.CreateTable() {
			if err = CreateTable(ctx, dwh, tableData.Mode(), targetTableConfig, opts.ColumnSettings, targetTableID, false, targetKeysMissing); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dwh, targetTableConfig, opts.ColumnSettings, targetTableID, targetKeysMissing); err != nil {
				return fmt.Errorf("failed to add columns for table %q: %w", targetTableID.Table(), err)
			}
		}
	}

	if msmSettings.FlushCount == 0 {
		// If it's the first time we're doing this, we should now prepare the MSM table and be done.
		if err = dwh.PrepareTemporaryTable(ctx, tableData, msmTableConfig, msmTableID, msmTableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, true); err != nil {
			return fmt.Errorf("failed to prepare temporary table: %w", err)
		}
	}

	return nil
}
