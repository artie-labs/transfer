package array

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/stringutil"
)

func InterfaceToArrayString(val any, recastAsArray bool) ([]string, error) {
	if val == nil {
		return nil, nil
	}

	list := reflect.ValueOf(val)
	if list.Kind() != reflect.Slice {
		if recastAsArray {
			// Since it's not a slice, let's cast it as a slice and re-enter this function.
			return InterfaceToArrayString([]any{val}, recastAsArray)
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
			valMap, isOk := value.(map[string]any)
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
			vals = append(vals, stringutil.EscapeBackslashes(fmt.Sprint(value)))
		}
	}

	return vals, nil
}

func StringsJoinAddSingleQuotes(values []string) string {
	var vals []string
	for _, value := range values {
		vals = append(vals, fmt.Sprintf(`'%s'`, value))
	}

	return strings.Join(vals, ",")
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
