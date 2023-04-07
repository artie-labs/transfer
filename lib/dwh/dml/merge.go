package dml

import (
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

func MergeStatement(fqTableName, subQuery, pk, idempotentKey string, cols []string, softDelete bool, specialStructCastMergeKey bool) (string, error) {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	var idempotentClause string
	if idempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND cc.%s >= c.%s ", idempotentKey, idempotentKey)
	}

	equalitySQL := fmt.Sprintf("c.%s = cc.%s", pk, pk)
	if specialStructCastMergeKey {
		// BigQuery requires special casting to compare two JSON objects.
		equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", pk, pk)
	}

	if softDelete {
		return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on %s
				when matched %sthen UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, fqTableName, subQuery, equalitySQL,
			// Update + Soft Deletion
			idempotentClause, array.ColumnsUpdateQuery(cols, "cc"),
			// Insert
			constants.DeleteColumnMarker, strings.Join(cols, ","),
			array.StringsJoinAddPrefix(cols, ",", "cc.")), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	for idx, col := range cols {
		if col == constants.DeleteColumnMarker {
			cols = append(cols[:idx], cols[idx+1:]...)
			removed = true
			break
		}
	}

	if !removed {
		return "", errors.New("artie delete flag doesn't exist")
	}

	return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on %s
				when matched AND cc.%s then DELETE
				when matched AND IFNULL(cc.%s, false) = false %sthen UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, fqTableName, subQuery, equalitySQL,
		// Delete
		constants.DeleteColumnMarker,
		// Update
		constants.DeleteColumnMarker, idempotentClause, array.ColumnsUpdateQuery(cols, "cc"),
		// Insert
		constants.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(cols, ",", "cc.")), nil

}
