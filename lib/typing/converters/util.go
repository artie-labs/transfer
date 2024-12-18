package converters

import "strconv"

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
