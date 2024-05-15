package sql

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func QuoteColumns(cols []columns.Column, dialect Dialect) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = dialect.QuoteIdentifier(col.Name())
	}
	return result
}

func QuoteTableAliasColumn(tableAlias constants.TableAlias, column columns.Column, dialect Dialect) string {
	return fmt.Sprintf("%s.%s", tableAlias, dialect.QuoteIdentifier(column.Name()))
}

// buildColumnsUpdateFragment will parse the columns and then returns a list of strings like: first_name=tgt.first_name,last_name=stg.last_name,email=tgt.email
// NOTE: This should only be used with valid columns.
func BuildColumnsUpdateFragment(columns []columns.Column, stagingAlias, targetAlias constants.TableAlias, dialect Dialect) string {
	var cols []string
	for _, column := range columns {
		var colVal string
		if column.ToastColumn {
			colVal = fmt.Sprintf(" CASE WHEN %s THEN %s ELSE %s END",
				dialect.BuildIsNotToastValueExpression(stagingAlias, column),
				QuoteTableAliasColumn(stagingAlias, column, dialect),
				QuoteTableAliasColumn(targetAlias, column, dialect),
			)
		} else {
			colVal = QuoteTableAliasColumn(stagingAlias, column, dialect)
		}
		cols = append(cols, fmt.Sprintf("%s=%s", dialect.QuoteIdentifier(column.Name()), colVal))
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
