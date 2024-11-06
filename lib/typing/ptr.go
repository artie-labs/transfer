package typing

func ToPtr[T any](v T) *T {
	return &v
}

func DefaultValueFromPtr[T any](value *T, defaultValue T) T {
	if value == nil {
		return defaultValue
	}

	return *value
}
