package dml

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"strings"
)

func MergeStatement(fqTableName, subQuery, pk, idempotentKey string, cols []string) string {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.

	if idempotentKey == "" {
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
		`, fqTableName, subQuery, pk, pk,
			// Delete
			constants.DeleteColumnMarker,
			// Update
			constants.DeleteColumnMarker, array.ColumnsUpdateQuery(cols, "cc"),
			// Insert
			constants.DeleteColumnMarker, strings.Join(cols, ","),
			array.StringsJoinAddPrefix(cols, ",", "cc."))
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
		`, fqTableName, subQuery, pk, pk,
		// Delete
		constants.DeleteColumnMarker, idempotentKey, idempotentKey,
		// Update
		constants.DeleteColumnMarker, idempotentKey, idempotentKey, array.ColumnsUpdateQuery(cols, "cc"),
		// Insert
		constants.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(cols, ",", "cc."))
}
