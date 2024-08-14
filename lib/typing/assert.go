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
