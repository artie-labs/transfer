package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"strings"
)

// ParseSchemaQuery is to parse out the results from this query: https://cloud.google.com/bigquery/docs/information-schema-tables#example_1
func ParseSchemaQuery(rows map[string]string, createTable bool) (*types.DwhTableConfig, error) {
	ddlVal, isOk := rows["ddl"]
	if !isOk {
		// If the rows don't exist, that's normal if the table doesn't exist.
		if createTable {
			return &types.DwhTableConfig{
				CreateTable: createTable,
			}, nil
		}

		return nil, fmt.Errorf("missing ddl column")
	}

	// TrimSpace only does the L + R side.
	ddlString := strings.TrimSpace(fmt.Sprint(ddlVal))

	leftBracketIdx := strings.Index(ddlString, "(")
	if leftBracketIdx < 0 || (leftBracketIdx+1) > len(ddlString) {
		return nil, fmt.Errorf("malformed DDL string: %s", ddlString)
	}

	// Sometimes the DDL statement has OPTIONS, sometimes it doesn't.
	// The cases we need to care for:
	// 1) CREATE TABLE `foo` (col_1 string, col_2 string) OPTIONS (...);
	// 2) CREATE TABLE `foo` (col_1 string, col_2 string)OPTIONS (...);
	// 3) CREATE TABLE `foo` (col_1 string, col_2 string);
	optionsIdx := strings.Index(ddlString, "OPTIONS")
	if optionsIdx < 0 {
		// This means, optionsIdx doesn't exist, so let's defer to finding the end of the statement (;).
		optionsIdx = len(ddlString)
	}

	if optionsIdx < 0 {
		return nil, fmt.Errorf("malformed DDL string1, %s", ddlString)
	}

	if leftBracketIdx == optionsIdx {
		return nil, fmt.Errorf("malformed DDL string2, %s", ddlString)
	}

	ddlString = ddlString[leftBracketIdx+1 : optionsIdx]
	endOfStatement := strings.LastIndex(ddlString, ")")
	if endOfStatement < 0 || (endOfStatement-1) < 0 {
		return nil, fmt.Errorf("malformed DDL string3, %s", ddlString)
	}

	tableToColumnTypes := make(map[string]typing.Kind)
	ddlString = ddlString[:endOfStatement]
	columnsToTypes := strings.Split(ddlString, ",")
	for _, colType := range columnsToTypes {
		// TrimSpace will clear spaces, \t, \n for both L+R side of the string.
		parts := strings.Split(strings.TrimSpace(colType), " ")
		if len(parts) < 2 {
			return nil, fmt.Errorf("unexpected colType, colType: %s, parts: %v", colType, parts)
		}

		tableToColumnTypes[parts[0]] = typing.BigQueryTypeToKind(strings.Join(parts[1:], " "))
	}

	return types.NewDwhTableConfig(tableToColumnTypes, nil, createTable), nil
}
