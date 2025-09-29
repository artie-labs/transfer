package typing

import "fmt"

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
