package maputil

import (
	"fmt"
	"strconv"
)

func GetKeyFromMap(obj map[string]any, key string, defaultValue any) any {
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

func GetTypeFromMap[T any](obj map[string]any, key string) (T, error) {
	var zero T
	if len(obj) == 0 {
		return zero, fmt.Errorf("object is empty")
	}

	valInterface, isOk := obj[key]
	if !isOk {
		return zero, fmt.Errorf("key: %q does not exist in object", key)
	}

	val, isOk := valInterface.(T)
	if !isOk {
		return zero, fmt.Errorf("expected key %q to be type string, got %T", key, valInterface)
	}

	return val, nil
}

func GetTypeFromMapWithDefault[T any](obj map[string]any, key string, defaultValue T) (T, error) {
	if _, isOk := obj[key]; !isOk {
		// If the value doesn't exist, then return default
		return defaultValue, nil
	}

	return GetTypeFromMap[T](obj, key)
}
