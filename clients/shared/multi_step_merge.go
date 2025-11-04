package shared

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func MultiStepMerge(ctx context.Context, dest destination.Destination, tableData *optimization.TableData, opts types.MergeOpts) (bool, error) {
	if _, ok := dest.Dialect().(dialect.SnowflakeDialect); !ok {
		return false, fmt.Errorf("multi-step merge is only supported on Snowflake")
	}

	msmSettings := tableData.MultiStepMergeSettings()
	if !msmSettings.Enabled {
		return false, fmt.Errorf("multi-step merge is not enabled")
	}

	if tableData.ShouldSkipUpdate() {
		// TODO: We should support the fact that if we've written data to the msm table and there are no messages in subsequent flushes,
		// we should merge the msm table into the target table.
		return false, nil
	}

	msmTableID := dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), fmt.Sprintf("%s_%s_msm", constants.ArtiePrefix, tableData.Name()))
	msmTableID = msmTableID.WithTemporaryTable(true)
	targetTableID := dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	targetTableConfig, err := dest.GetTableConfig(ctx, targetTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	if msmSettings.IsFirstFlush() {
		// If it's the first time we are doing this, we should ensure the MSM table has been dropped.
		if err := dest.DropTable(ctx, msmTableID); err != nil {
			return false, fmt.Errorf("failed to drop msm table: %w", err)
		}

		// We should now align our columns against the target table.
		if err = tableData.MergeColumnsFromDestination(targetTableConfig.GetColumns()...); err != nil {
			return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
		}
	}

	msmTableConfig, err := dest.GetTableConfig(ctx, msmTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}
	{
		// Apply schema evolution for the MSM table
		resp := columns.Diff(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			msmTableConfig.GetColumns(),
		)

		if msmTableConfig.CreateTable() {
			if err = CreateTable(ctx, dest, tableData.Mode(), msmTableConfig, opts.ColumnSettings, msmTableID, true, resp.TargetColumnsMissing); err != nil {
				return false, fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dest, msmTableConfig, opts.ColumnSettings, msmTableID, resp.TargetColumnsMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for table %q: %w", msmTableID.Table(), err)
			}
		}
	}
	{
		// Apply schema evolution for the target table
		// TODO: Support dropping columns for the target table.
		_, targetKeysMissing := columns.DiffAndFilter(
			tableData.ReadOnlyInMemoryCols().GetColumns(),
			targetTableConfig.GetColumns(),
			tableData.BuildColumnsToKeep(),
		)

		if targetTableConfig.CreateTable() {
			if err = CreateTable(ctx, dest, tableData.Mode(), targetTableConfig, opts.ColumnSettings, targetTableID, false, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dest, targetTableConfig, opts.ColumnSettings, targetTableID, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for table %q: %w", targetTableID.Table(), err)
			}
		}
	}

	if msmSettings.IsFirstFlush() {
		// If it's the first flush, we'll just load the data directly into the MSM table.
		// Don't need to create the temporary table, we've already created it above.
		if err = dest.LoadDataIntoTable(ctx, tableData, msmTableConfig, msmTableID, msmTableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, false); err != nil {
			return false, fmt.Errorf("failed to prepare temporary table: %w", err)
		}
	} else {
		// Upon subsequent flushes, we'll want to load data into a staging table and then merge it into the MSM table.
		temporaryTableID := TempTableIDWithSuffix(targetTableID, tableData.TempTableSuffix())
		opts.UseBuildMergeQueryIntoStagingTable = true
		opts.PrepareTemporaryTable = true
		if err := merge(ctx, dest, tableData, msmTableConfig, temporaryTableID, msmTableID, opts); err != nil {
			return false, fmt.Errorf("failed to merge msm table into target table: %w", err)
		}

		if msmSettings.IsLastFlush() {
			// If it's the last flush, we'll want to load the MSM table into the target table.
			opts.UseBuildMergeQueryIntoStagingTable = false
			opts.PrepareTemporaryTable = false
			if err := merge(ctx, dest, tableData, targetTableConfig, msmTableID, targetTableID, opts); err != nil {
				return false, fmt.Errorf("failed to merge msm table into target table: %w", err)
			}

			// We should only commit on the last flush.
			return true, nil
		}
	}

	tableData.WipeData()
	tableData.IncrementMultiStepMergeFlushCount()
	slog.Info("Multi-step merge completed, updated the flush count and wiped our in-memory database", slog.Int("flushCount", tableData.MultiStepMergeSettings().FlushCount()))
	return false, nil
}

func merge(ctx context.Context, dwh destination.Destination, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, temporaryTableID, targetTableID sql.TableIdentifier, opts types.MergeOpts) error {
	defer func() {
		if dropErr := ddl.DropTemporaryTable(ctx, dwh, temporaryTableID, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableID.FullyQualifiedName()))
		}
	}()

	if opts.PrepareTemporaryTable {
		if err := dwh.LoadDataIntoTable(ctx, tableData, tableConfig, temporaryTableID, targetTableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, true); err != nil {
			return fmt.Errorf("failed to prepare temporary table: %w", err)
		}
	}

	// TODO: Support column backfill
	subQuery := temporaryTableID.FullyQualifiedName()
	if opts.SubQueryDedupe {
		subQuery = dwh.Dialect().BuildDedupeTableQuery(temporaryTableID, tableData.PrimaryKeys())
	}

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

	var mergeStatements []string
	if opts.UseBuildMergeQueryIntoStagingTable {
		mergeStatements = dwh.Dialect().BuildMergeQueryIntoStagingTable(
			targetTableID,
			subQuery,
			primaryKeys,
			opts.AdditionalEqualityStrings,
			validColumns,
		)
	} else {
		_mergeStatements, err := dwh.Dialect().BuildMergeQueries(
			targetTableID,
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

		mergeStatements = _mergeStatements
	}

	if _, err := destination.ExecContextStatements(ctx, dwh, mergeStatements); err != nil {
		return fmt.Errorf("failed to execute merge statements: %w", err)
	}

	return nil
}
