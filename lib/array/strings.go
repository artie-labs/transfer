package array

import (
	"fmt"
	"strings"
)

type StringsJoinAddPrefixArgs struct {
	Vals      []string
	Separator string
	Prefix    string
	Suffix    string
}

// StringsJoinAddPrefix will add a prefix to vals, then join all the parts together via the separator
// Use case for this is to add a prefix for the tableName to all the columns
func StringsJoinAddPrefix(args StringsJoinAddPrefixArgs) string {
	var retVals []string
	for _, val := range args.Vals {
		retVals = append(retVals, args.Prefix+val+args.Suffix)
	}

	return strings.Join(retVals, args.Separator)
}

// ColumnsUpdateQuery will take a list of columns + tablePrefix and return
// columnA = tablePrefix.columnA, columnB = tablePrefix.columnB. This is the Update syntax that Snowflake requires
func ColumnsUpdateQuery(columns []string, tablePrefix string) string {
	// NOTE: columns and sflkCols must be the same.
	var _columns []string

	for _, column := range columns {
		// This is to make it look like: objCol = cc.objCol::variant
		_columns = append(_columns, fmt.Sprintf("%s=%s.%s", column, tablePrefix, column))
	}

	return strings.Join(_columns, ",")
}

// Empty will iterate over a list, if one of the item in the list is empty, it will return true
// This is useful to check the presence of a setting.
func Empty(list []string) bool {
	for _, v := range list {
		if empty := v == ""; empty {
			return true
		}
	}

	return false
}

// StringContains iterates over a list, if any `item` from the list matches `element`, it returns true.
func StringContains(list []string, element string) bool {
	for _, v := range list {
		if element == v {
			return true
		}
	}

	return false
}

func RemoveElement(list []string, elementToRemove string) []string {
	for index, element := range list {
		if element == elementToRemove {
			return append(list[:index], list[index+1:]...)
		}
	}
	return list
}
