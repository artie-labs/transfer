package maputil

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
)

// GetKeyFromMap - If the key does not exists, it will return the default value.
func GetKeyFromMap(obj map[string]any, key string, defaultValue any) any {
	if len(obj) == 0 {
		return defaultValue
	}

	val, ok := obj[key]
	if !ok {
		return defaultValue
	}

	return val
}

func GetInt32FromMap(obj map[string]any, key string) (int32, error) {
	if len(obj) == 0 {
		return 0, fmt.Errorf("object is empty")
	}

	valInterface, ok := obj[key]
	if !ok {
		return 0, fmt.Errorf("key: %s does not exist in object", key)
	}

	val, err := strconv.ParseInt(fmt.Sprint(valInterface), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("key: %s is not type integer: %w", key, err)
	}

	return int32(val), nil
}

func GetTypeFromMap[T any](obj map[string]any, key string) (T, error) {
	value, ok := obj[key]
	if !ok {
		var zero T
		return zero, fmt.Errorf("key: %q does not exist in object", key)
	}

	return typing.AssertType[T](value)
}

// GetCaseInsensitiveValue performs a case-insensitive lookup in the map
func GetCaseInsensitiveValue(m map[string]any, key string) (any, bool) {
	// First try exact match
	if value, ok := m[key]; ok {
		return value, true
	}

	// Then try case-insensitive match
	lowerKey := strings.ToLower(key)
	for k, v := range m {
		if strings.ToLower(k) == lowerKey {
			return v, true
		}
	}

	return nil, false
}
