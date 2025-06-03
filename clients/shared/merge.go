package shared

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const backfillMaxRetries = 1000

func Merge(ctx context.Context, dest destination.Destination, tableData *optimization.TableData, opts types.MergeOpts) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	tableConfig, err := dest.GetTableConfig(tableID, tableData.TopicConfig().DropDeletedColumns)
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

	if err = AlterTableDropColumns(ctx, dest, tableConfig, tableID, srcKeysMissing, tableData.LatestCDCTs, tableData.ContainOtherOperations()); err != nil {
		return fmt.Errorf("failed to drop columns for table %q: %w", tableID.Table(), err)
	}

	// TODO: Examine whether [AuditColumnsToDelete] still needs to be called.
	tableConfig.AuditColumnsToDelete(srcKeysMissing)
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

	return nil
}
