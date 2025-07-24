package primitives

import (
	"fmt"
	"math"
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
	case float64:
		// We'll check for overflow and make sure there's no precision loss
		if castValue > math.MaxInt64 || castValue < math.MinInt64 {
			return 0, fmt.Errorf("value %f overflows int64", castValue)
		}

		if math.Trunc(castValue) != castValue {
			return 0, fmt.Errorf("float64 (%f) has fractional component", castValue)
		}

		return int64(castValue), nil
	}

	return 0, fmt.Errorf("failed to parse int64, unsupported type: %T", value)
}

type BooleanConverter struct{}

func (BooleanConverter) Convert(value any) (bool, error) {
	switch castValue := value.(type) {
	case string:
		return strconv.ParseBool(castValue)
	case bool:
		return castValue, nil
	}

	return false, fmt.Errorf("failed to parse boolean, unsupported type: %T", value)
}
