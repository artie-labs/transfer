package snowflake

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

// escapeCols will return the following arguments:
// 1) colsToUpdate - list of columns to update
// 2) list of columns to update (escaped).
func escapeCols(cols []typing.Column) (colsToUpdate []string, colsToUpdateEscaped []string) {
	for _, column := range cols {
		if column.KindDetails.Kind == typing.Invalid.Kind {
			// Don't update Snowflake
			continue
		}

		escapedCol := column.Name
		switch column.KindDetails.Kind {
		case typing.Struct.Kind, typing.Array.Kind:
			if column.ToastColumn {
				escapedCol = fmt.Sprintf("CASE WHEN %s = '%s' THEN {'key': '%s'} ELSE PARSE_JSON(%s) END %s",
					// Comparing the column against placeholder
					column.Name, constants.ToastUnavailableValuePlaceholder,
					// Casting placeholder as a JSON object
					constants.ToastUnavailableValuePlaceholder,
					// Regular parsing.
					column.Name, column.Name)
			} else {
				escapedCol = fmt.Sprintf("PARSE_JSON(%s) %s", column.Name, column.Name)
			}
		}

		colsToUpdate = append(colsToUpdate, column.Name)
		colsToUpdateEscaped = append(colsToUpdateEscaped, escapedCol)
	}

	return
}

func getMergeStatement(tableData *optimization.TableData) (string, error) {
	var tableValues []string
	colsToUpdate, colsToUpdateEscaped := escapeCols(tableData.ReadOnlyInMemoryCols().GetColumns())
	for _, value := range tableData.RowsData() {
		var rowValues []string
		for _, col := range colsToUpdate {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal := value[col]
			if colVal != nil {
				switch colKind.KindDetails.Kind {
				// All the other types do not need string wrapping.
				case typing.ETime.Kind:
					extTime, err := ext.ParseFromInterface(colVal)
					if err != nil {
						return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
					}

					switch extTime.NestedKind.Type {
					case ext.TimeKindType:
						colVal = stringutil.Wrap(extTime.String(ext.PostgresTimeFormatNoTZ), false)
					default:
						colVal = stringutil.Wrap(extTime.String(""), false)
					}

				case typing.String.Kind, typing.Struct.Kind:
					colVal = stringutil.Wrap(colVal, false)
				case typing.Array.Kind:
					// We need to marshall, so we can escape the strings.
					// https://go.dev/play/p/BcCwUSCeTmT
					colValBytes, err := json.Marshal(colVal)
					if err != nil {
						return "", err
					}

					colVal = stringutil.Wrap(string(colValBytes), false)
				}
			} else {
				colVal = "null"
			}

			rowValues = append(rowValues, fmt.Sprint(colVal))
		}

		tableValues = append(tableValues, fmt.Sprintf("(%s) ", strings.Join(rowValues, ",")))
	}

	subQuery := fmt.Sprintf("SELECT %s FROM (values %s) as %s(%s)", strings.Join(colsToUpdateEscaped, ","),
		strings.Join(tableValues, ","), tableData.Name(), strings.Join(colsToUpdate, ","))

	return dml.MergeStatement(dml.MergeArgument{
		FqTableName:    tableData.ToFqName(constants.Snowflake),
		SubQuery:       subQuery,
		IdempotentKey:  tableData.IdempotentKey,
		PrimaryKeys:    tableData.PrimaryKeys,
		Columns:        colsToUpdate,
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.SoftDelete,
	})
}
