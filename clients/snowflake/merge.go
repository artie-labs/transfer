package snowflake

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/dml"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func getMergeStatement(tableData *optimization.TableData) (string, error) {
	var tableValues []string
	var cols []string
	var sflkCols []string

	// Given all the columns, diff this against SFLK.
	for _, column := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if column.KindDetails.Kind == typing.Invalid.Kind {
			// Don't update Snowflake
			continue
		}

		sflkCol := column.Name
		switch column.KindDetails.Kind {
		case typing.Struct.Kind, typing.Array.Kind:
			if column.ToastColumn {
				sflkCol = fmt.Sprintf("CASE WHEN %s = '%s' THEN {'key': '%s'} ELSE PARSE_JSON(%s) END %s",
					// Comparing the column against placeholder
					column.Name, constants.ToastUnavailableValuePlaceholder,
					// Casting placeholder as a JSON object
					constants.ToastUnavailableValuePlaceholder,
					// Regular parsing.
					column.Name, column.Name)
			} else {
				sflkCol = fmt.Sprintf("PARSE_JSON(%s) %s", column.Name, column.Name)
			}
		}

		cols = append(cols, column.Name)
		sflkCols = append(sflkCols, sflkCol)
	}

	for _, value := range tableData.RowsData() {
		var rowValues []string
		for _, col := range cols {
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
						colVal = stringutil.Wrap(extTime.String(ext.PostgresTimeFormatNoTZ))
					default:
						colVal = stringutil.Wrap(extTime.String(""))
					}

				case typing.String.Kind, typing.Struct.Kind:
					colVal = stringutil.Wrap(colVal)
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

			rowValues = append(rowValues, fmt.Sprint(colVal))
		}

		tableValues = append(tableValues, fmt.Sprintf("(%s) ", strings.Join(rowValues, ",")))
	}

	subQuery := fmt.Sprintf("SELECT %s FROM (values %s) as %s(%s)", strings.Join(sflkCols, ","),
		strings.Join(tableValues, ","), tableData.TopicConfig.TableName, strings.Join(cols, ","))

	return dml.MergeStatement(dml.MergeArgument{
		FqTableName:    tableData.ToFqName(constants.Snowflake),
		SubQuery:       subQuery,
		IdempotentKey:  tableData.IdempotentKey,
		PrimaryKeys:    tableData.PrimaryKeys,
		Columns:        cols,
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.SoftDelete,
	})
}
