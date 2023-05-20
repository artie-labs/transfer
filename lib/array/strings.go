package array

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

func InterfaceToArrayStringEscaped(val interface{}) ([]string, error) {
	if val == nil {
		return nil, nil
	}

	list := reflect.ValueOf(val)
	if list.Kind() != reflect.Slice {
		return nil, fmt.Errorf("wrong data type")
	}

	var vals []string
	for i := 0; i < list.Len(); i++ {
		kind := list.Index(i).Kind()
		value := list.Index(i).Interface()
		var shouldParse bool
		if kind == reflect.Interface {
			valMap, isOk := value.(map[string]interface{})
			if isOk {
				value = valMap
			}

			shouldParse = true
		}

		if kind == reflect.Map || kind == reflect.Struct || shouldParse {
			bytes, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}

			vals = append(vals, string(bytes))
		} else {
			vals = append(vals, stringutil.WrapNoQuotes(value))
		}
	}

	return vals, nil
}

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

// ColumnsUpdateQuery takes:
// columns - list of columns to iterate
// columnsToTypes - given that list, provide the types (separate list because this list may contain invalid columns
// bigQueryTypeCasting - We'll need to escape the column comparison if the column's a struct.
// It then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
func ColumnsUpdateQuery(columns []string, columnsToTypes typing.Columns, bigQueryTypeCasting bool) string {
	var _columns []string
	for _, column := range columns {
		columnType, isOk := columnsToTypes.GetColumn(column)
		if isOk && columnType.ToastColumn {
			if columnType.KindDetails == typing.Struct {
				if bigQueryTypeCasting {
					_columns = append(_columns,
						fmt.Sprintf(`%s= CASE WHEN TO_JSON_STRING(cc.%s) != '{"key": "%s"}' THEN cc.%s ELSE c.%s END`,
							// col CASE when TO_JSON_STRING(cc.col) != { 'key': TOAST_UNAVAILABLE_VALUE }
							column, column, constants.ToastUnavailableValuePlaceholder,
							// cc.col ELSE c.col END
							column, column))
				} else {
					_columns = append(_columns,
						fmt.Sprintf("%s= CASE WHEN cc.%s != {'key': '%s'} THEN cc.%s ELSE c.%s END",
							// col CASE WHEN cc.col
							column, column,
							// { 'key': TOAST_UNAVAILABLE_VALUE } THEN cc.col ELSE c.col END",
							constants.ToastUnavailableValuePlaceholder, column, column))
				}
			} else {
				// t.column3 = CASE WHEN t.column3 != '__debezium_unavailable_value' THEN t.column3 ELSE s.column3 END
				_columns = append(_columns,
					fmt.Sprintf("%s= CASE WHEN cc.%s != '%s' THEN cc.%s ELSE c.%s END",
						// col = CASE WHEN cc.col != TOAST_UNAVAILABLE_VALUE
						column, column, constants.ToastUnavailableValuePlaceholder,
						// THEN cc.col ELSE c.col END
						column, column))
			}

		} else {
			// This is to make it look like: objCol = cc.objCol
			_columns = append(_columns, fmt.Sprintf("%s=cc.%s", column, column))
		}

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
