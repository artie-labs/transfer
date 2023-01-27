package bigquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
)

// ParseSchemaQuery is to parse out the results from this query: https://cloud.google.com/bigquery/docs/information-schema-tables#example_1
func ParseSchemaQuery(rows map[string]string) (*types.DwhTableConfig, error) {
	ddlVal, isOk := rows["ddl"]
	if !isOk {
		return nil, fmt.Errorf("missing ddl column")
	}

	// TrimSpace only does the L + R side.
	ddlString := strings.TrimSpace(fmt.Sprint(ddlVal))

	backtickIdx := strings.LastIndex(ddlString, "`")
	if backtickIdx < 0 || (backtickIdx+2) > len(ddlString) {
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
		return nil, fmt.Errorf("malformed DDL string, %s", ddlString)
	}

	ddlString = ddlString[backtickIdx+2 : optionsIdx]
	endOfStatement := strings.LastIndex(ddlString, ")")
	if endOfStatement < 0 || (endOfStatement-1) < 0 {
		return nil, fmt.Errorf("malformed DDL string, %s", ddlString)
	}

	ddlString = ddlString[:endOfStatement]
	fmt.Println("ddlString", ddlString)
	dwhTableConfig := &types.DwhTableConfig{
		Columns:         make(map[string]typing.Kind),
		ColumnsToDelete: make(map[string]time.Time),
		CreateTable:     false,
	}

	columnsToTypes := strings.Split(ddlString, ",")
	for _, colType := range columnsToTypes {
		parts := strings.Split(colType, " ")
		if len(parts) < 2 {
			return nil, fmt.Errorf("unexpected colType, colType: %s, parts: %v", colType, parts)
		}

		dwhTableConfig.Columns[parts[0]] = typing.BigQueryTypeToKind(strings.Join(parts[1:], " "))
	}

	return dwhTableConfig, nil
}
