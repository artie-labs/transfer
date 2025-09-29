package typing

import (
	"encoding/json"
	"fmt"
	"strings"
)

func AssertType[T any](val any) (T, error) {
	castedVal, ok := val.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("expected type %T, got %T", zero, val)
	}
	return castedVal, nil
}

// AssertTypeOptional - will return zero if the value is nil, otherwise it will assert the type
func AssertTypeOptional[T any](val any) (T, error) {
	var zero T
	if val == nil {
		return zero, nil
	}

	return AssertType[T](val)
}

func ToPtr[T any](v T) *T {
	return &v
}

func DefaultValueFromPtr[T any](value *T, defaultValue T) T {
	if value == nil {
		return defaultValue
	}

	return *value
}

// IsJSON - We also need to check if the string is a JSON string or not
// If it could be one, it will start with { and end with }.
// Once there, we will then check if it's a JSON string or not.
// This is an optimization since JSON string checking is expensive.
func IsJSON(str string) bool {
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return false
	}

	firstChar := str[0]
	if firstChar != '{' && firstChar != '[' {
		return false
	}
	return json.Valid([]byte(str))
}
