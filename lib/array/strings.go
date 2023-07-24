package array

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
)

func InterfaceToArrayString(val interface{}, recastAsArray bool) ([]string, error) {
	if val == nil {
		return nil, nil
	}

	list := reflect.ValueOf(val)
	if list.Kind() != reflect.Slice {
		if recastAsArray {
			// Since it's not a slice, let's cast it as a slice and re-enter this function.
			return InterfaceToArrayString([]interface{}{val}, recastAsArray)
		} else {
			return nil, fmt.Errorf("wrong data type, kind: %v", list.Kind())
		}

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
			vals = append(vals, stringutil.Wrap(value, true))
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
