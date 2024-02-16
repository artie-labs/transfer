package shared

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func Merge(dwh destination.DataWarehouse, tableData *optimization.TableData, cfg config.Config, opts types.MergeOpts) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	fqName := tableData.ToFqName(dwh.Label(), true, cfg.SharedDestinationConfig.UppercaseEscapedNames, "")
	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return err
	}

	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt,
		tableData.TopicConfig.IncludeDatabaseUpdatedAt, tableData.Mode())

	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               dwh,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &cfg.SharedDestinationConfig.UppercaseEscapedNames,
	}

	// Columns that are missing in DWH, but exist in our CDC stream.
	err = ddl.AlterTable(createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	// Keys that exist in DWH, but not in our CDC stream.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:                    dwh,
		Tc:                     tableConfig,
		FqTableName:            fqName,
		CreateTable:            false,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: tableData.ContainOtherOperations(),
		CdcTime:                tableData.LatestCDCTs,
		UppercaseEscNames:      &cfg.SharedDestinationConfig.UppercaseEscapedNames,
	}

	if err = ddl.AlterTable(deleteAlterTableArgs, srcKeysMissing...); err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(dwh.Label(), false, cfg.SharedDestinationConfig.UppercaseEscapedNames, ""), tableData.TempTableSuffix())
	if err = dwh.PrepareTemporaryTable(tableData, tableConfig, temporaryTableName, types.AdditionalSettings{}); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	defer func() {
		if dropErr := ddl.DropTemporaryTable(dwh, temporaryTableName, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableName))
		}
	}()

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		var attempts int
		for {
			err = BackfillColumn(cfg, dwh, col, fqName)
			if err == nil {
				tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
					Backfilled: ptr.ToBool(true),
				})
				break
			}

			if opts.RetryColBackfill && dwh.IsRetryableError(err) {
				err = nil
				attempts += 1
				time.Sleep(jitter.Jitter(1500, jitter.DefaultMaxMs, attempts))
			} else {
				return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %w", col.RawName(), col.RawDefaultValue(), err)
			}
		}

	}

	subQuery := temporaryTableName
	if opts.SubQueryDedupe {
		subQuery = fmt.Sprintf(`( SELECT DISTINCT * FROM %s )`, temporaryTableName)
	}

	mergeArg := dml.MergeArgument{
		FqTableName:       fqName,
		SubQuery:          subQuery,
		IdempotentKey:     tableData.TopicConfig.IdempotentKey,
		PrimaryKeys:       tableData.PrimaryKeys(cfg.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: dwh.Label()}),
		ColumnsToTypes:    *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:        tableData.TopicConfig.SoftDelete,
		DestKind:          dwh.Label(),
		UppercaseEscNames: &cfg.SharedDestinationConfig.UppercaseEscapedNames,
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
			_, err = tx.Exec(mergeQuery)
			if err != nil {
				return fmt.Errorf("failed to merge, query: %v, err: %w", mergeQuery, err)
			}
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to merge, parts: %v, err: %w", mergeParts, err)
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
