package typing

import "fmt"

func AssertType[T any](val any) (T, error) {
	castedVal, isOk := val.(T)
	if !isOk {
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
