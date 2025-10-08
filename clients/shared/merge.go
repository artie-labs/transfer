package shared

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const (
	backfillMaxRetries     = 1000
	heartbeatsInitialDelay = 30 * time.Minute
	heartbeatsInterval     = 2 * time.Minute
)

func Merge(ctx context.Context, dest destination.Destination, tableData *optimization.TableData, opts types.MergeOpts) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	hb := lib.NewHeartbeats(heartbeatsInitialDelay, heartbeatsInterval, "merge", map[string]any{
		"table":   tableData.Name(),
		"tableID": tableID.FullyQualifiedName(),
	})

	stop := hb.Start()
	defer stop()

	tableConfig, err := dest.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	srcKeysMissing, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.BuildColumnsToKeep(),
	)

	if tableConfig.CreateTable() {
		if err = CreateTable(ctx, dest, tableData.Mode(), tableConfig, opts.ColumnSettings, tableID, false, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err = AlterTableAddColumns(ctx, dest, tableConfig, opts.ColumnSettings, tableID, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to add columns for table %q: %w", tableID.Table(), err)
		}
	}

	if err = AlterTableDropColumns(ctx, dest, tableConfig, tableID, srcKeysMissing, tableData.GetLatestTimestamp(), tableData.ContainOtherOperations()); err != nil {
		return fmt.Errorf("failed to drop columns for table %q: %w", tableID.Table(), err)
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w for table %q", err, tableData.Name())
	}

	temporaryTableID := TempTableIDWithSuffix(dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name()), tableData.TempTableSuffix())
	defer func() {
		if dropErr := ddl.DropTemporaryTable(ctx, dest, temporaryTableID, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableID.FullyQualifiedName()))
		}
	}()

	if err = dest.PrepareTemporaryTable(ctx, tableData, tableConfig, temporaryTableID, tableID, types.AdditionalSettings{ColumnSettings: opts.ColumnSettings}, true); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	// Now iterate over all the in-memory cols and see which ones require a backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		var backfillErr error
		for attempts := 0; attempts < backfillMaxRetries; attempts++ {
			backfillErr = BackfillColumn(ctx, dest, col, tableID)
			if backfillErr == nil {
				err = tableConfig.UpsertColumn(col.Name(), columns.UpsertColumnArg{
					Backfilled: typing.ToPtr(true),
				})

				if err != nil {
					return fmt.Errorf("failed to update column backfilled status: %w", err)
				}

				break
			}

			if opts.RetryColBackfill && dest.IsRetryableError(backfillErr) {
				sleepDuration := jitter.Jitter(1500, jitter.DefaultMaxMs, attempts)
				slog.Warn("Failed to apply backfill, retrying...", slog.Any("err", backfillErr),
					slog.Duration("sleep", sleepDuration), slog.Int("attempts", attempts))
				time.Sleep(sleepDuration)
			} else {
				break
			}
		}

		if backfillErr != nil {
			return fmt.Errorf("failed to backfill col: %s, default value: %v, err: %w", col.Name(), col.DefaultValue(), backfillErr)
		}
	}

	subQuery := temporaryTableID.FullyQualifiedName()
	if opts.SubQueryDedupe {
		subQuery = dest.Dialect().BuildDedupeTableQuery(temporaryTableID, tableData.PrimaryKeys())
	}

	return ExecuteMergeOperations(ctx, dest, tableData, tableID, subQuery, opts)
}
