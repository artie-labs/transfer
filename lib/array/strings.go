package array

import (
	"fmt"
	"strings"
)

// StringsJoinAddPrefix will add a prefix to vals, then join all the parts together via the separator
// Use case for this is to add a prefix for the tableName to all the columns
func StringsJoinAddPrefix(vals []string, separator string, prefix string) string {
	var retVals []string
	for _, val := range vals {
		retVals = append(retVals, prefix+val)
	}

	return strings.Join(retVals, separator)
}

// ColumnsUpdateQuery will take a list of columns + tablePrefix and return
// columnA = tablePrefix.columnA, columnB = tablePrefix.columnB. This is the Update syntax that Snowflake requires
func ColumnsUpdateQuery(columns []string, tablePrefix string) string {
	// NOTE: columns and sflkCols must be the same.
	var _columns []string

	for _, column := range columns {
		// This is to make it look like: objCol = cc.objCol::variant
		_columns = append(_columns, fmt.Sprintf("%s = %s.%s", column, tablePrefix, column))
	}

	return strings.Join(_columns, ",")
}

func StringContains(list []string, element string) bool {
	for _, v := range list {
		if element == v {
			return true
		}
	}

	return false

}
