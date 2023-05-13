package dml

import (
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type MergeArgument struct {
	FqTableName   string
	SubQuery      string
	IdempotentKey string
	PrimaryKeys   []string
	// Columns has been stripped to remove any invalid columns.
	// TODO - Do we need both?
	Columns        []string
	ColumnsToTypes typing.Columns

	// SpecialCastingRequired - This is used for columns that have JSON value. This is required for BigQuery
	// We will be casting the value in this column as such: `TO_JSON_STRING(<columnName>)`
	SpecialCastingRequired bool
	SoftDelete             bool
}

// generateMatchClause - TODO add comment
func (m *MergeArgument) generateMatchClause() string {
	var toastedCols []string
	// We'll need to iterate over potential columns and check if any of the columns contain TOAST values
	// If they do, then we'll need to update our MATCH statement to include and exclude them.
	for _, col := range m.Columns {
		colKind, isOk := m.ColumnsToTypes.GetColumn(col)
		if isOk {
			if colKind.ToastColumn {
				toastedCols = append(toastedCols, col)
			}
		}
	}

	matchedString := "when matched"
	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	if m.IdempotentKey != "" {
		matchedString = fmt.Sprintf("%s AND cc.%s >= c.%s", matchedString, m.IdempotentKey, m.IdempotentKey)
	}

	if len(toastedCols) > 0 {
		// We will need to escape TOASTED columns.
		//matchedString = fmt.Sprintf("%s AND %s", array.StringsJoinAddPrefix(toastedCols, ""))
	}

	return matchedString
}

func MergeStatement(m MergeArgument) (string, error) {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", primaryKey, primaryKey)
		pkCol, isOk := m.ColumnsToTypes.GetColumn(primaryKey)
		if !isOk {
			return "", fmt.Errorf("error: column: %s does not exist in columnToType: %v", primaryKey, m.ColumnsToTypes)
		}

		if m.SpecialCastingRequired && pkCol.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", primaryKey, primaryKey)
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	if m.SoftDelete {
		return fmt.Sprintf(`
			MERGE INTO %s c using (%s) as cc on %s
				%s
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
			m.generateMatchClause(),
			// Insert
			constants.DeleteColumnMarker, strings.Join(m.Columns, ","),
			array.StringsJoinAddPrefix(
				array.StringsJoinAddPrefixArgs{
					Vals:      m.Columns,
					Separator: ",",
					Prefix:    "cc.",
				}),
		), nil
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
				%s
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
		m.generateMatchClause(),
		// Insert
		constants.DeleteColumnMarker, strings.Join(m.Columns, ","),
		array.StringsJoinAddPrefix(
			array.StringsJoinAddPrefixArgs{
				Vals:      m.Columns,
				Separator: ",",
				Prefix:    "cc.",
			}),
	), nil

}
