package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/columns"
)

func QuoteColumns(cols []columns.Column, dialect Dialect) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = dialect.QuoteIdentifier(col.Name())
	}
	return result
}

func QuoteTableAliasColumns(tableAlias string, cols []columns.Column, dialect Dialect) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = fmt.Sprintf("%s.%s", tableAlias, dialect.QuoteIdentifier(col.Name()))
	}
	return result
}

// buildColumnsUpdateFragment will parse the columns and then returns a list of strings like: first_name=tgt.first_name,last_name=stg.last_name,email=tgt.email
// NOTE: This should only be used with valid columns.
func BuildColumnsUpdateFragment(columns []columns.Column, stagingAlias, targetAlias string, dialect Dialect) string {
	var cols []string
	for _, column := range columns {
		colName := dialect.QuoteIdentifier(column.Name())
		if column.ToastColumn {
			cols = append(cols, fmt.Sprintf("%s= CASE WHEN %s THEN %s.%s ELSE %s.%s END",
				colName, dialect.BuildIsNotToastValueExpression(stagingAlias, column), stagingAlias, colName, targetAlias, colName))
		} else {
			// This is to make it look like: objCol=stg.objCol
			cols = append(cols, fmt.Sprintf("%s=%s.%s", colName, stagingAlias, colName))
		}
	}

	return strings.Join(cols, ",")
}

type Operator string

const (
	Equal              Operator = "="
	GreaterThanOrEqual Operator = ">="
)

func BuildColumnComparison(column columns.Column, table1, table2 string, operator Operator, dialect Dialect) string {
	quotedColumnName := dialect.QuoteIdentifier(column.Name())
	return fmt.Sprintf("%s.%s %s %s.%s", table1, quotedColumnName, operator, table2, quotedColumnName)
}

func BuildColumnComparisons(_columns []columns.Column, table1, table2 string, operator Operator, dialect Dialect) []string {
	var result = make([]string, len(_columns))
	for i, column := range _columns {
		result[i] = BuildColumnComparison(column, table1, table2, operator, dialect)
	}
	return result
}
