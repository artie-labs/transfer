package snowflake

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func stringWrapping(colVal interface{}) string {
	// Escape line breaks, JSON_PARSE does not like it.
	colVal = strings.ReplaceAll(fmt.Sprint(colVal), `\`, `\\`)
	// The normal string escape is to do for O'Reilly is O\\'Reilly, but Snowflake escapes via \'
	return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprint(colVal), "'", `\'`))
}

func merge(tableData *optimization.TableData) (string, error) {
	var tableValues []string
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
		for _, col := range cols {
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

	return dml.MergeStatement(tableData.ToFqName(constants.Snowflake), subQuery,
		tableData.PrimaryKey, tableData.IdempotentKey, cols, tableData.SoftDelete)
}
