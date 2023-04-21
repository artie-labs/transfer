package dml

import (
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type MergeArgument struct {
	// TODO: Test
	FqTableName   string
	SubQuery      string
	IdempotentKey string
	PrimaryKeys   []string
	// TODO refactor columns -> parsedColumns
	Columns []string

	//  TODO rename columnToType --> columns
	ColumnToType typing.Columns

	// SpecialCastingRequired - This is used for columns that have JSON value. This is required for BigQuery
	// We will be casting the value in this column as such: `TO_JSON_STRING(<columnName>)`
	SpecialCastingRequired bool
	SoftDelete             bool
}

func MergeStatement(m MergeArgument) (string, error) {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	var idempotentClause string
	if m.IdempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND cc.%s >= c.%s ", m.IdempotentKey, m.IdempotentKey)
	}

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)
		pkCol := m.ColumnToType.GetColumn(primaryKey)
		if pkCol == nil {
			return "", fmt.Errorf("error: column: %s does not exist in columnToType: %v", primaryKey, m.ColumnToType)
		}

		if pkCol.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", primaryKey, primaryKey)
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	if m.SoftDelete {
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
		`, m.FqTableName, m.SubQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, array.ColumnsUpdateQuery(m.Columns, "cc"),
			// Insert
			constants.DeleteColumnMarker, strings.Join(m.Columns, ","),
			array.StringsJoinAddPrefix(m.Columns, ",", "cc.")), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	for idx, col := range m.Columns {
		if col == constants.DeleteColumnMarker {
			m.Columns = append(m.Columns[:idx], m.Columns[idx+1:]...)
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
		`, m.FqTableName, m.SubQuery, strings.Join(equalitySQLParts, " and "),
		// Delete
		constants.DeleteColumnMarker,
		// Update
		constants.DeleteColumnMarker, idempotentClause, array.ColumnsUpdateQuery(m.Columns, "cc"),
		// Insert
		constants.DeleteColumnMarker, strings.Join(m.Columns, ","),
		array.StringsJoinAddPrefix(m.Columns, ",", "cc.")), nil

}
