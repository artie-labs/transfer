package shared

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const backfillMaxRetries = 1000

func Merge(dwh destination.DataWarehouse, tableData *optimization.TableData, cfg config.Config, opts types.MergeOpts) error {
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
		Dwh:               dwh,
		Tc:                tableConfig,
		TableID:           tableID,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: ptr.ToBool(dwh.ShouldUppercaseEscapedNames()),
		Mode:              tableData.Mode(),
	}

	// Columns that are missing in DWH, but exist in our CDC stream.
	err = createAlterTableArgs.AlterTable(targetKeysMissing...)
	if err != nil {
		return fmt.Errorf("failed to alter table: %w", err)
	}

	// Keys that exist in DWH, but not in our CDC stream.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:                    dwh,
		Tc:                     tableConfig,
		TableID:                tableID,
		CreateTable:            false,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: tableData.ContainOtherOperations(),
		CdcTime:                tableData.LatestCDCTs,
		UppercaseEscNames:      ptr.ToBool(dwh.ShouldUppercaseEscapedNames()),
		Mode:                   tableData.Mode(),
	}

	if err = deleteAlterTableArgs.AlterTable(srcKeysMissing...); err != nil {
		return fmt.Errorf("failed to apply alter table: %w", err)
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
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
			backfillErr = BackfillColumn(cfg, dwh, col, tableID)
			if backfillErr == nil {
				tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
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
			return fmt.Errorf("failed to backfill col: %s, default value: %v, err: %w", col.RawName(), col.RawDefaultValue(), backfillErr)
		}
	}

	subQuery := temporaryTableName
	if opts.SubQueryDedupe {
		subQuery = fmt.Sprintf(`( SELECT DISTINCT * FROM %s )`, temporaryTableName)
	}

	mergeArg := dml.MergeArgument{
		TableID:             tableID,
		SubQuery:            subQuery,
		IdempotentKey:       tableData.TopicConfig().IdempotentKey,
		PrimaryKeys:         tableData.PrimaryKeys(dwh.ShouldUppercaseEscapedNames(), &sql.NameArgs{Escape: true, DestKind: dwh.Label()}),
		Columns:             tableData.ReadOnlyInMemoryCols(),
		SoftDelete:          tableData.TopicConfig().SoftDelete,
		DestKind:            dwh.Label(),
		UppercaseEscNames:   ptr.ToBool(dwh.ShouldUppercaseEscapedNames()),
		ContainsHardDeletes: ptr.ToBool(tableData.ContainsHardDeletes()),
	}

	if len(opts.AdditionalEqualityStrings) > 0 {
		mergeArg.AdditionalEqualityStrings = opts.AdditionalEqualityStrings
	}

	if opts.UseMergeParts {
		mergeParts, err := mergeArg.GetParts()
		if err != nil {
			return fmt.Errorf("failed to generate merge statement: %w", err)
		}

		tx, err := dwh.Begin()
		if err != nil {
			return fmt.Errorf("failed to start tx: %w", err)
		}

		for _, mergeQuery := range mergeParts {
			if _, err = tx.Exec(mergeQuery); err != nil {
				return fmt.Errorf("failed to merge, query: %v, err: %w", mergeQuery, err)
			}
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to merge, parts: %v, err: %w", mergeParts, err)
		}

		return nil
	} else if dwh.Label() == constants.MSSQL {
		mergeQuery, err := mergeArg.GetMSSQLStatement()
		if err != nil {
			return fmt.Errorf("failed to generate merge statement: %w", err)
		}

		slog.Debug("Executing...", slog.String("query", mergeQuery))
		_, err = dwh.Exec(mergeQuery)
		if err != nil {
			return fmt.Errorf("failed to execute merge: %w", err)
		}

		return nil
	} else {
		mergeQuery, err := mergeArg.GetStatement()
		if err != nil {
			return fmt.Errorf("failed to generate merge statement: %w", err)
		}

		slog.Debug("Executing...", slog.String("query", mergeQuery))
		_, err = dwh.Exec(mergeQuery)
		return err
	}
}
