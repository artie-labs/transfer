package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/artie-labs/transfer/lib/stringutil"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func merge(tableData *optimization.TableData) (string, error) {
	var cols []string
	// Given all the columns, diff this against SFLK.
	for _, col := range tableData.InMemoryColumns.GetColumns() {
		if col.KindDetails == typing.Invalid {
			// Don't update BQ
			continue
		}

		cols = append(cols, col.Name)
	}

	var rowValues []string
	firstRow := true
	for _, value := range tableData.RowsData {
		var colVals []string
		for _, col := range cols {
			colKind := tableData.InMemoryColumns.GetColumn(col)
			colVal := value[col]
			if colVal != nil {
				switch colKind.KindDetails.Kind {
				case typing.ETime.Kind:
					extTime, err := ext.ParseFromInterface(colVal)
					if err != nil {
						return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
					}

					switch extTime.NestedKind.Type {
					case ext.DateTimeKindType:
						colVal = fmt.Sprintf("PARSE_DATETIME('%s', '%v')", RFC3339Format, extTime.String(time.RFC3339Nano))
					case ext.DateKindType:
						colVal = fmt.Sprintf("PARSE_DATE('%s', '%v')", PostgresDateFormat, extTime.String(ext.Date.Format))
					case ext.TimeKindType:
						colVal = fmt.Sprintf("PARSE_TIME('%s', '%v')", PostgresTimeFormatNoTZ, extTime.String(ext.PostgresTimeFormatNoTZ))
					}
				// All the other types do not need string wrapping.
				case typing.String.Kind, typing.Struct.Kind:
					colVal = stringutil.Wrap(colVal)
					if colKind.KindDetails == typing.Struct {
						// This is how you cast string -> JSON
						colVal = fmt.Sprintf("JSON %s", colVal)
					}
				case typing.Array.Kind:
					// We need to marshall, so we can escape the strings.
					// https://go.dev/play/p/BcCwUSCeTmT
					colValBytes, err := json.Marshal(colVal)
					if err != nil {
						return "", err
					}

					colVal = stringutil.Wrap(string(colValBytes))
				}
			} else {
				colVal = "null"
			}

			if firstRow {
				colVal = fmt.Sprintf("%v as %s", colVal, col)
			}

			colVals = append(colVals, fmt.Sprint(colVal))
		}

		firstRow = false
		rowValues = append(rowValues, fmt.Sprintf("SELECT %s", strings.Join(colVals, ",")))
	}

	subQuery := strings.Join(rowValues, " UNION ALL ")

	return dml.MergeStatement(dml.MergeArgument{
		FqTableName:   tableData.ToFqName(constants.BigQuery),
		SubQuery:      subQuery,
		IdempotentKey: tableData.IdempotentKey,
		PrimaryKeys:   tableData.PrimaryKeys,
		Columns:       cols,
		ColumnToType:  *tableData.InMemoryColumns,
		SoftDelete:    tableData.SoftDelete,
		// BigQuery specifically needs it.
		SpecialCastingRequired: true,
	})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.InMemoryColumns == nil {
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
	srcKeysMissing, targetKeysMissing := typing.Diff(*tableData.InMemoryColumns, targetColumns, tableData.SoftDelete)

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(ctx, s, tableConfig, tableData.ToFqName(constants.BigQuery), tableConfig.CreateTable, constants.Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = ddl.AlterTable(ctx, s, tableConfig, tableData.ToFqName(constants.BigQuery), false, constants.Delete, tableData.LatestCDCTs, srcKeysMissing...)
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

	tableData.UpdateInMemoryColumns(tableConfig.Columns().GetColumns()...)
	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.Exec(query)
	return err
}
