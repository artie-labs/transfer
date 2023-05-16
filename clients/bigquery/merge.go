package bigquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func merge(tableData *optimization.TableData) (string, error) {
	var cols []string
	// Given all the columns, diff this against SFLK.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.KindDetails == typing.Invalid {
			// Don't update BQ
			continue
		}

		cols = append(cols, col.Name)
	}

	var rowValues []string
	firstRow := true

	for _, value := range tableData.RowsData() {
		var colVals []string
		for _, col := range cols {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := CastColVal(value[col], colKind)
			if err != nil {
				return "", err
			}

			if firstRow {
				colVal = fmt.Sprintf("%v as %s", colVal, col)
			}

			colVals = append(colVals, colVal)
		}

		firstRow = false
		rowValues = append(rowValues, fmt.Sprintf("SELECT %s", strings.Join(colVals, ",")))
	}

	subQuery := strings.Join(rowValues, " UNION ALL ")

	return dml.MergeStatement(dml.MergeArgument{
		FqTableName:         tableData.ToFqName(constants.BigQuery),
		SubQuery:            subQuery,
		IdempotentKey:       tableData.IdempotentKey,
		PrimaryKeys:         tableData.PrimaryKeys,
		Columns:             cols,
		ColumnsToTypes:      *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:          tableData.SoftDelete,
		BigQueryTypeCasting: true,
	})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	tableConfig, err := s.getTableConfig(ctx, tableData)
	if err != nil {
		return err
	}

	var targetColumns typing.Columns
	if tableConfig.Columns() != nil {
		targetColumns = *tableConfig.Columns()
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(*tableData.ReadOnlyInMemoryCols(), targetColumns, tableData.SoftDelete)

	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: tableData.ToFqName(constants.BigQuery),
		CreateTable: tableConfig.CreateTable,
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: tableData.ToFqName(constants.BigQuery),
		CreateTable: false,
		ColumnOp:    constants.Delete,
		CdcTime:     tableData.LatestCDCTs,
	}

	err = ddl.AlterTable(ctx, deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ColumnsToDelete() {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			tableConfig.ClearColumnsToDeleteByColName(colToDelete)
		}
	}

	tableData.UpdateInMemoryColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.Exec(query)
	return err
}
