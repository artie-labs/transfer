package shared

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func MultiStepMerge(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.MergeOpts) (bool, error) {
	if _, ok := dwh.Dialect().(dialect.SnowflakeDialect); !ok {
		return false, fmt.Errorf("multi-step merge is only supported on Snowflake")
	}

	msmSettings := tableData.MultiStepMergeSettings()
	if !msmSettings.Enabled {
		return false, fmt.Errorf("multi-step merge is not enabled")
	}

	if tableData.ShouldSkipUpdate() {
		// TODO: We should support the fact that if we've written data to the msm table and there are no messages in subsequent attempts,
		// we should merge the msm table into the target table.
		return false, nil
	}

	msmTableID := dwh.IdentifierFor(tableData.TopicConfig(), fmt.Sprintf("%s_msm", tableData.Name()))
	targetTableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	targetTableConfig, err := dwh.GetTableConfig(targetTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
	}

	if msmSettings.FirstAttempt() {
		// If it's the first time we are doing this, we should ensure the MSM table has been dropped.
		if err := dwh.DropTable(ctx, msmTableID); err != nil {
			return false, fmt.Errorf("failed to drop msm table: %w", err)
		}

		// We should now align our columns against the target table.
		if err = tableData.MergeColumnsFromDestination(targetTableConfig.GetColumns()...); err != nil {
			return false, fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
		}
	}

	msmTableConfig, err := dwh.GetTableConfig(msmTableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return false, fmt.Errorf("failed to get table config: %w", err)
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
				return false, fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dwh, msmTableConfig, opts.ColumnSettings, msmTableID, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for table %q: %w", msmTableID.Table(), err)
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
				return false, fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			if err = AlterTableAddColumns(ctx, dwh, targetTableConfig, opts.ColumnSettings, targetTableID, targetKeysMissing); err != nil {
				return false, fmt.Errorf("failed to add columns for table %q: %w", targetTableID.Table(), err)
			}
		}
	}

	if msmSettings.FirstAttempt() {
		// If it's the first time we're doing this, we should now prepare the MSM table and be done.
		if err = dwh.PrepareTemporaryTable(ctx, tableData, msmTableConfig, msmTableID, msmTableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, true); err != nil {
			return false, fmt.Errorf("failed to prepare temporary table: %w", err)
		}
	} else {
		// Now we'll want to load the staging table into the MSM table
		// If it's the last attempt, we'll want to load the MSM table into the target table.
		if err := merge(ctx, dwh, tableData, msmTableConfig, msmTableID, opts); err != nil {
			return false, fmt.Errorf("failed to merge msm table into target table: %w", err)
		}

		if msmSettings.LastAttempt() {
			if err := merge(ctx, dwh, tableData, targetTableConfig, targetTableID, opts); err != nil {
				return false, fmt.Errorf("failed to merge msm table into target table: %w", err)
			}

			// We should only commit on the last attempt.
			return true, nil
		}
	}

	tableData.WipeData()
	tableData.UpdateMultiStepMergeAttempt()
	return false, nil
}

func merge(ctx context.Context, dwh destination.DataWarehouse, tableData *optimization.TableData, tableConfig *types.DwhTableConfig, targetTableID sql.TableIdentifier, opts types.MergeOpts) error {
	temporaryTableID := TempTableIDWithSuffix(targetTableID, tableData.TempTableSuffix())
	defer func() {
		if dropErr := ddl.DropTemporaryTable(dwh, temporaryTableID, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableID.FullyQualifiedName()))
		}
	}()

	if err := dwh.PrepareTemporaryTable(ctx, tableData, tableConfig, temporaryTableID, targetTableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, true); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
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

	mergeStatements, err := dwh.Dialect().BuildMergeQueries(
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

	if err = destination.ExecStatements(dwh, mergeStatements); err != nil {
		return fmt.Errorf("failed to execute merge statements: %w", err)
	}

	return nil
}
