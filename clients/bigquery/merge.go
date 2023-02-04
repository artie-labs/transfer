package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"strings"
	"time"
)

func merge(tableData *optimization.TableData) (string, error) {
	var artieDeleteMetadataIdx *int
	var cols []string
	// Given all the columns, diff this against SFLK.
	for col, kind := range tableData.Columns {
		if kind == typing.Invalid {
			// Don't update BQ
			continue
		}

		cols = append(cols, col)
	}

	var rowValues []string
	firstRow := true
	for _, value := range tableData.RowsData {
		var colVals []string
		for idx, col := range cols {
			// Hasn't been set yet and the column is the DELETE flag. We want to remove this from
			// the final table because this is a flag, not an actual column.
			if artieDeleteMetadataIdx == nil && col == constants.DeleteColumnMarker {
				artieDeleteMetadataIdx = ptr.ToInt(idx)
			}

			colKind := tableData.Columns[col]
			colVal := value[col]
			if colVal != nil {
				switch colKind {
				case typing.DateTime:
					ts, err := typing.ParseDateTime(fmt.Sprint(colVal))
					if err != nil {
						return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
					}

					// We need to re-cast the timestamp INTO ISO-8601.
					colVal = fmt.Sprintf("PARSE_DATETIME('%s', '%v')", RFC3339Format, ts.Format(time.RFC3339Nano))
				// All the other types do not need string wrapping.
				case typing.String, typing.Struct:
					// Escape line breaks, JSON_PARSE does not like it.
					colVal = strings.ReplaceAll(fmt.Sprint(colVal), `\n`, `\\n`)
					// The normal string escape is to do for O'Reilly is O\\'Reilly, but Snowflake escapes via \'
					colVal = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprint(colVal), "'", `\'`))
					if colKind == typing.Struct {
						// This is how you cast string -> JSON
						colVal = fmt.Sprintf("JSON %s", colVal)
					}
				case typing.Array:
					// We need to marshall, so we can escape the strings.
					// https://go.dev/play/p/BcCwUSCeTmT
					colValBytes, err := json.Marshal(colVal)
					if err != nil {
						return "", err
					}

					colVal = fmt.Sprintf("%s", strings.ReplaceAll(string(colValBytes), "'", `\'`))
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

	if artieDeleteMetadataIdx == nil {
		return "", errors.New("artie delete flag doesn't exist")
	}

	// Hide the deletion flag
	cols = append(cols[:*artieDeleteMetadataIdx], cols[*artieDeleteMetadataIdx+1:]...)

	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	if tableData.IdempotentKey == "" {
		return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on c.%s = cc.%s
				when matched AND cc.%s then DELETE
				when matched AND IFNULL(cc.%s, false) = false then UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, tableData.ToFqName(constants.BigQuery), subQuery, tableData.PrimaryKey, tableData.PrimaryKey,
			// Delete
			constants.DeleteColumnMarker,
			// Update
			constants.DeleteColumnMarker, array.ColumnsUpdateQuery(cols, "cc"),
			// Insert
			constants.DeleteColumnMarker, strings.Join(cols, ","),
			array.StringsJoinAddPrefix(cols, ",", "cc.")), nil
	}

	return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on c.%s = cc.%s
				when matched AND cc.%s AND cc.%s >= c.%s = true then DELETE
				when matched AND IFNULL(cc.%s, false) = false AND cc.%s >= c.%s then UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, tableData.ToFqName(constants.BigQuery), subQuery, tableData.PrimaryKey, tableData.PrimaryKey,
		// Delete
		constants.DeleteColumnMarker, tableData.IdempotentKey, tableData.IdempotentKey,
		// Update
		constants.DeleteColumnMarker, tableData.IdempotentKey, tableData.IdempotentKey, array.ColumnsUpdateQuery(cols, "cc"),
		// Insert
		constants.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(cols, ",", "cc.")), nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows == 0 {
		// There's no rows. Let's skip.
		return nil
	}

	tableConfig, err := s.GetTableConfig(ctx, tableData)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := typing.Diff(tableData.Columns, tableConfig.Columns())

	// Keys that exist in CDC stream, but not in Snowflake
	err = s.alterTable(ctx, tableData.ToFqName(constants.BigQuery), tableConfig.CreateTable, constants.Add, tableData.LatestCDCTs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	err = s.alterTable(ctx, tableData.ToFqName(constants.BigQuery), false, constants.Delete, tableData.LatestCDCTs, srcKeysMissing...)
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

	query, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	log.WithField("query", query).Debug("executing...")
	_, err = s.Exec(query)
	return err
}
