package snowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
)

func stringWrapping(colVal interface{}) string {
	// Escape line breaks, JSON_PARSE does not like it.
	colVal = strings.ReplaceAll(fmt.Sprint(colVal), `\n`, `\\n`)
	// The normal string escape is to do for O'Reilly is O\\'Reilly, but Snowflake escapes via \'
	return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprint(colVal), "'", `\'`))
}

func merge(tableData *optimization.TableData) (string, error) {
	var tableValues []string
	var artieDeleteMetadataIdx *int
	var cols []string
	var sflkCols []string

	// Given all the columns, diff this against SFLK.
	for col, kindDetails := range tableData.InMemoryColumns {
		if kindDetails.Kind == typing.Invalid.Kind {
			// Don't update Snowflake
			continue
		}

		sflkCol := col
		switch kindDetails.Kind {
		case typing.Struct.Kind, typing.Array.Kind:
			sflkCol = fmt.Sprintf("PARSE_JSON(%s) %s", col, col)
		}

		cols = append(cols, col)
		sflkCols = append(sflkCols, sflkCol)
	}

	for _, value := range tableData.RowsData {
		var rowValues []string
		for idx, col := range cols {
			// Hasn't been set yet and the column is the DELETE flag. We want to remove this from
			// the final table because this is a flag, not an actual column.
			if artieDeleteMetadataIdx == nil && col == constants.DeleteColumnMarker {
				artieDeleteMetadataIdx = ptr.ToInt(idx)
			}

			colKind := tableData.InMemoryColumns[col]
			colVal := value[col]
			if colVal != nil {
				switch colKind.Kind {
				// All the other types do not need string wrapping.
				case typing.ETime.Kind:
					extTime, err := ext.ParseFromInterface(colVal)
					if err != nil {
						return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
					}

					switch extTime.NestedKind.Type {
					case ext.TimeKindType:
						colVal = stringWrapping(extTime.String(ext.PostgresTimeFormatNoTZ))
					default:
						colVal = stringWrapping(extTime.String(""))
					}

				case typing.String.Kind, typing.Struct.Kind:
					colVal = stringWrapping(colVal)
				case typing.Array.Kind:
					// We need to marshall, so we can escape the strings.
					// https://go.dev/play/p/BcCwUSCeTmT
					colValBytes, err := json.Marshal(colVal)
					if err != nil {
						return "", err
					}

					colVal = fmt.Sprintf("'%s'", strings.ReplaceAll(string(colValBytes), "'", `\'`))
				}
			} else {
				colVal = "null"
			}

			rowValues = append(rowValues, fmt.Sprint(colVal))
		}

		tableValues = append(tableValues, fmt.Sprintf("(%s) ", strings.Join(rowValues, ",")))
	}

	subQuery := fmt.Sprintf("SELECT %s FROM (values %s) as %s(%s)", strings.Join(sflkCols, ","),
		strings.Join(tableValues, ","), tableData.TopicConfig.TableName, strings.Join(cols, ","))

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
		`, tableData.ToFqName(constants.Snowflake), subQuery, tableData.PrimaryKey, tableData.PrimaryKey,
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
		`, tableData.ToFqName(constants.Snowflake), subQuery, tableData.PrimaryKey, tableData.PrimaryKey,
		// Delete
		constants.DeleteColumnMarker, tableData.IdempotentKey, tableData.IdempotentKey,
		// Update
		constants.DeleteColumnMarker, tableData.IdempotentKey, tableData.IdempotentKey, array.ColumnsUpdateQuery(cols, "cc"),
		// Insert
		constants.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(cols, ",", "cc.")), nil
}
