package snowflake

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
)

func merge(tableData *optimization.TableData) (string, error) {
	var tableValues []string
	var artieDeleteMetadataIdx *int
	var cols []string
	// Given all the columns, diff this against SFLK.
	for col, kind := range tableData.Columns {
		if kind == typing.Invalid {
			// Don't update Snowflake
			continue
		}

		cols = append(cols, col)
	}

	for _, value := range tableData.RowsData {
		var rowValues []string
		for idx, col := range cols {
			// Hasn't been set yet and the column is the DELETE flag. We want to remove this from
			// the final table because this is a flag, not an actual column.
			if artieDeleteMetadataIdx == nil && col == config.DeleteColumnMarker {
				artieDeleteMetadataIdx = ptr.ToInt(idx)
			}

			colVal := value[col]
			if colVal != nil {
				// TODO: Test some gnarly string.
				if reflect.TypeOf(colVal).String() == "string" {
					colVal = fmt.Sprintf("'%s'", strings.Replace(fmt.Sprint(colVal), "'", `\'`, -1))
				}
			} else {
				colVal = "null"
			}

			rowValues = append(rowValues, fmt.Sprint(colVal))
		}

		tableValues = append(tableValues, fmt.Sprintf("(%s) ", strings.Join(rowValues, ",")))
	}

	subQuery := fmt.Sprintf("SELECT * FROM (values %s) as %s(%s)", strings.Join(tableValues, ","),
		tableData.TopicConfig.TableName, strings.Join(cols, ","))

	if artieDeleteMetadataIdx == nil {
		return "", errors.New("artie delete flag doesn't exist")
	}

	cols = append(cols[:*artieDeleteMetadataIdx], cols[*artieDeleteMetadataIdx+1:]...)
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// If this assumption is wrong in the future, we can revisit.

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on c.%s = cc.%s
				when matched AND cc.%s = true then DELETE
				when matched AND IFNULL(cc.%s, false) = false and cc.%s >= c.%s then UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, tableData.ToFqName(), subQuery, tableData.PrimaryKey, tableData.PrimaryKey,
		config.DeleteColumnMarker, config.DeleteColumnMarker, tableData.IdempotentKey,
		tableData.IdempotentKey, array.ColumnsUpdateQuery(cols, "cc"),
		config.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(cols, ",", "cc.")), nil
}
