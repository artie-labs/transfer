package shared

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const backfillMaxRetries = 1000

func Merge(dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.MergeOpts) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig().SoftDelete, tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt, tableData.Mode())

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	createAlterTableArgs := ddl.AlterTableArgs{
		Dialect:     dwh.Dialect(),
		Tc:          tableConfig,
		TableID:     tableID,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
		Mode:        tableData.Mode(),
	}

	// Columns that are missing in DWH, but exist in our CDC stream.
	err = createAlterTableArgs.AlterTable(dwh, targetKeysMissing...)
	if err != nil {
		return fmt.Errorf("failed to alter table: %w", err)
	}

	// Keys that exist in DWH, but not in our CDC stream.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dialect:                dwh.Dialect(),
		Tc:                     tableConfig,
		TableID:                tableID,
		CreateTable:            false,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: tableData.ContainOtherOperations(),
		CdcTime:                tableData.LatestCDCTs,
		Mode:                   tableData.Mode(),
	}

	if err = deleteAlterTableArgs.AlterTable(dwh, srcKeysMissing...); err != nil {
		return fmt.Errorf("failed to apply alter table: %w", err)
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)
	if err = tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	temporaryTableID := TempTableID(dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name()), tableData.TempTableSuffix())
	temporaryTableName := temporaryTableID.FullyQualifiedName()
	if err = dwh.PrepareTemporaryTable(tableData, tableConfig, temporaryTableID, types.AdditionalSettings{}, true); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	defer func() {
		if dropErr := ddl.DropTemporaryTable(dwh, temporaryTableName, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableName))
		}
	}()

	// Now iterate over all the in-memory cols and see which ones require a backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		var backfillErr error
		for attempts := 0; attempts < backfillMaxRetries; attempts++ {
			backfillErr = BackfillColumn(dwh, col, tableID)
			if backfillErr == nil {
				tableConfig.Columns().UpsertColumn(col.Name(), columns.UpsertColumnArg{
					Backfilled: ptr.ToBool(true),
				})
				break
			}

			if opts.RetryColBackfill && dwh.IsRetryableError(backfillErr) {
				sleepDuration := jitter.Jitter(1500, jitter.DefaultMaxMs, attempts)
				slog.Warn("Failed to apply backfill, retrying...", slog.Any("err", backfillErr),
					slog.Duration("sleep", sleepDuration), slog.Int("attempts", attempts))
				time.Sleep(sleepDuration)
			} else {
				break
			}
		}

		if backfillErr != nil {
			return fmt.Errorf("failed to backfill col: %s, default value: %v, err: %w", col.Name(), col.RawDefaultValue(), backfillErr)
		}
	}

	subQuery := temporaryTableName
	if opts.SubQueryDedupe {
		subQuery = fmt.Sprintf(`( SELECT DISTINCT * FROM %s )`, temporaryTableName)
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

	mergeArg := dml.MergeArgument{
		TableID:             tableID,
		SubQuery:            subQuery,
		IdempotentKey:       tableData.TopicConfig().IdempotentKey,
		PrimaryKeys:         primaryKeys,
		Columns:             cols.ValidColumns(),
		SoftDelete:          tableData.TopicConfig().SoftDelete,
		Dialect:             dwh.Dialect(),
		ContainsHardDeletes: ptr.ToBool(tableData.ContainsHardDeletes()),
	}

	if len(opts.AdditionalEqualityStrings) > 0 {
		mergeArg.AdditionalEqualityStrings = opts.AdditionalEqualityStrings
	}

	mergeStatements, err := mergeArg.BuildStatements()
	if err != nil {
		return fmt.Errorf("failed to generate merge statements: %w", err)
	}
	if err = destination.ExecStatements(dwh, mergeStatements); err != nil {
		return fmt.Errorf("failed to execute merge statements: %w", err)
	}
	return nil
}
