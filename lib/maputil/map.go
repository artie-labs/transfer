package maputil

import (
	"fmt"
	"strconv"
)

func GetKeyFromMap(obj map[string]any, key string, defaultValue any) any {
	if len(obj) == 0 {
		return defaultValue
	}

	val, isOk := obj[key]
	if !isOk {
		return defaultValue
	}

	return val
}

func GetInt32FromMap(obj map[string]any, key string) (int32, error) {
	if len(obj) == 0 {
		return 0, fmt.Errorf("object is empty")
	}

	valInterface, isOk := obj[key]
	if !isOk {
		return 0, fmt.Errorf("key: %s does not exist in object", key)
	}

	val, err := strconv.ParseInt(fmt.Sprint(valInterface), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("key: %s is not type integer: %w", key, err)
	}

	return int32(val), nil
}

func GetStringFromMap(row map[string]any, key string) (string, error) {
	val, isOk := row[key]
	if !isOk {
		return "", fmt.Errorf("key: %q does not exist in row: %v", key, row)
	}

	valString, isOk := val.(string)
	if !isOk {
		return "", fmt.Errorf("value: %v is not of type string, type: %T", val, val)
	}

	return valString, nil
}
