package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/typing"
	"strings"
	"time"
)

// ParseSchemaQuery is to parse out the results from this query: https://cloud.google.com/bigquery/docs/information-schema-tables#example_1
func ParseSchemaQuery(rows map[string]interface{}) (*types.DwhTableConfig, error) {
	/* The query looks something like this:

	CREATE TABLE `artie-labs.mock.customers`
	(
	  string_field_0 STRING,
	  string_field_1 STRING
	)
	OPTIONS(
	  expiration_timestamp=TIMESTAMP "2023-03-26T20:03:44.504Z"
	);

	*/

	ddlVal, isOk := rows["ddl"]
	if !isOk {
		return nil, fmt.Errorf("missing ddl column")
	}

	ddlString := fmt.Sprint(ddlVal)

	backtickIdx := strings.LastIndex(ddlString, "`")
	if backtickIdx < 0 || (backtickIdx+2) > len(ddlString) {
		return nil, fmt.Errorf("malformed DDL string: %s", ddlString)
	}

	// TODO test when there's no options.
	optionsIdx := strings.Index(ddlString, "OPTIONS")
	if optionsIdx < 0 || (optionsIdx-1) < 0 {
		return nil, fmt.Errorf("malformed DDL string, %s", ddlString)
	}

	dwhTableConfig := &types.DwhTableConfig{
		Columns:         make(map[string]typing.Kind),
		ColumnsToDelete: make(map[string]time.Time),
		CreateTable:     false,
	}

	columnsToTypes := strings.Split(ddlString[backtickIdx+2:optionsIdx-1], ",")
	for _, colType := range columnsToTypes {
		parts := strings.Split(colType, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("unexpected colType, colType: %s, parts: %v", colType, parts)
		}

		dwhTableConfig.Columns[parts[0]] = typing.BigQueryTypeToKind(parts[1])
	}

	return dwhTableConfig, nil
}
