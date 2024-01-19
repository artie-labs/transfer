package redshift

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	tableConfig, err := s.getTableConfig(tableData)
	if err != nil {
		return err
	}

	fqName := tableData.ToFqName(s.Label(), true, s.uppercaseEscNames, "")
	// Check if all the columns exist in Redshift
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &s.uppercaseEscNames,
	}

	// Keys that exist in CDC stream, but not in Redshift
	err = ddl.AlterTable(createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		slog.Warn("failed to apply alter table", slog.Any("err", err))
		return err
	}

	// Keys that exist in Redshift, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:                    s,
		Tc:                     tableConfig,
		FqTableName:            fqName,
		CreateTable:            false,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: tableData.ContainOtherOperations(),
		CdcTime:                tableData.LatestCDCTs,
		UppercaseEscNames:      &s.uppercaseEscNames,
	}

	err = ddl.AlterTable(deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		slog.Warn("failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)

	// Temporary tables cannot specify schemas, so we just prefix it instead.
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(s.Label(), false, s.uppercaseEscNames, ""), tableData.TempTableSuffix())
	if err = s.prepareTempTable(ctx, tableData, tableConfig, temporaryTableName); err != nil {
		return err
	}

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		err = utils.BackfillColumn(ctx, s, col, fqName)
		if err != nil {
			return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %v", col.RawName(), col.RawDefaultValue(), err)
		}

		tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
			Backfilled: ptr.ToBool(true),
		})
	}

	mergArg := dml.MergeArgument{
		FqTableName: fqName,
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQuery:          fmt.Sprintf(`( SELECT DISTINCT *  FROM %s )`, temporaryTableName),
		IdempotentKey:     tableData.TopicConfig.IdempotentKey,
		PrimaryKeys:       tableData.PrimaryKeys(s.uppercaseEscNames, &sql.NameArgs{Escape: true, DestKind: s.Label()}),
		ColumnsToTypes:    *tableData.ReadOnlyInMemoryCols(),
		SkipDelete:        tableData.TopicConfig.SkipDelete,
		SoftDelete:        tableData.TopicConfig.SoftDelete,
		DestKind:          s.Label(),
		UppercaseEscNames: &s.uppercaseEscNames,
	}

	// Prepare merge statement
	mergeParts, err := mergArg.GetParts()
	if err != nil {
		return fmt.Errorf("failed to generate merge statement, err: %v", err)
	}

	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to start tx, err: %v", err)
	}

	for _, mergeQuery := range mergeParts {
		_, err = tx.Exec(mergeQuery)
		if err != nil {
			return fmt.Errorf("failed to merge, query: %v, err: %v", mergeQuery, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to merge, parts: %v, err: %v", mergeParts, err)
	}

	_ = ddl.DropTemporaryTable(s, temporaryTableName, false)
	return err
}
