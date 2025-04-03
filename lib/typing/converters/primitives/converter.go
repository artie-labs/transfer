package primitives

import (
	"fmt"
	"strconv"
)

type Int64Converter struct{}

func (Int64Converter) Convert(value any) (int64, error) {
	switch castValue := value.(type) {
	case string:
		parsed, err := strconv.ParseInt(castValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse string to int64: %w", err)
		}
		return parsed, nil
	case int16:
		return int64(castValue), nil
	case int32:
		return int64(castValue), nil
	case int:
		return int64(castValue), nil
	case int64:
		return castValue, nil
	}
	return 0, fmt.Errorf("expected string/int/int16/int32/int64 got %T with value: %v", value, value)
}
