package converters

import (
	"fmt"
	"strconv"
)

func Float64ToString(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func Float32ToString(value float32) string {
	return strconv.FormatFloat(float64(value), 'f', -1, 32)
}

func BooleanToBit(val bool) int {
	if val {
		return 1
	} else {
		return 0
	}
}

func BitToBoolean[T int | int8 | int16 | int32 | int64](value T) (bool, error) {
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected value: %d, expected: [0, 1]", value)
	}
}
