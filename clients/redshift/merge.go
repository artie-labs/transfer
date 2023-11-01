package redshift

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	tableConfig, err := s.getTableConfig(ctx, getTableConfigArgs{
		TableData:          tableData,
		Schema:             tableData.TopicConfig.Schema,
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	fqName := tableData.ToFqName(ctx, s.Label(), true)
	// Check if all the columns exist in Redshift
	srcKeysMissing, targetKeysMissing := columns.Diff(ctx, tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in Redshift
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
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
	}

	err = ddl.AlterTable(ctx, deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name(ctx, nil) == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			tableConfig.ClearColumnsToDeleteByColName(colToDelete)
		}
	}

	tableData.UpdateInMemoryColumnsFromDestination(ctx, tableConfig.Columns().GetColumns()...)

	// Temporary tables cannot specify schemas, so we just prefix it instead.
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(ctx, s.Label(), false), tableData.TempTableSuffix())
	if err = s.prepareTempTable(ctx, tableData, tableConfig, temporaryTableName); err != nil {
		return err
	}

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		err = utils.BackfillColumn(ctx, s, col, tableData.ToFqName(ctx, s.Label(), true))
		if err != nil {
			defaultVal, _ := col.DefaultValue(ctx, nil)
			return fmt.Errorf("failed to backfill col: %v, default value: %v, error: %v",
				col.Name(ctx, nil), defaultVal, err)
		}

		tableConfig.Columns().UpsertColumn(col.Name(ctx, nil), columns.UpsertColumnArg{
			Backfilled: ptr.ToBool(true),
		})
	}

	// Prepare merge statement
	mergeParts, err := dml.MergeStatementParts(ctx, &dml.MergeArgument{
		FqTableName: fqName,
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQuery:      fmt.Sprintf(`( SELECT DISTINCT *  FROM %s )`, temporaryTableName),
		IdempotentKey: tableData.TopicConfig.IdempotentKey,
		PrimaryKeys: tableData.PrimaryKeys(ctx, &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SkipDelete:     tableData.TopicConfig.SkipDelete,
		SoftDelete:     tableData.TopicConfig.SoftDelete,
		DestKind:       s.Label(),
	})

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

	_ = ddl.DropTemporaryTable(ctx, s, temporaryTableName, false)
	return err
}
