package size

import (
	"fmt"
)

func GetApproxSize(value any) int {
	// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal.
	// We also chose not to use gob.NewEncoder because it does not work for all data types and had a huge computational overhead.
	// Another plus here is that this will not error out.
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case string:
		return len(v)
	case []byte:
		return len(v)
	case bool:
		return 1
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, int64, uint, uint64, uintptr, float64, complex64:
		// int, uint are platform dependent - but to be safe, let's over approximate and assume 64-bit system
		return 8
	case complex128:
		return 16
	case map[string]any:
		var size int
		for _, val := range v {
			size += GetApproxSize(val)
		}
		return size
	case []map[string]any:
		var size int
		for _, val := range v {
			size += GetApproxSize(val)
		}
		return size
	case []string:
		var size int
		for _, val := range v {
			size += GetApproxSize(val)
		}
		return size
	case []any:
		var size int
		for _, val := range v {
			size += GetApproxSize(val)
		}
		return size
	case [][]byte:
		var size int
		for _, val := range v {
			size += GetApproxSize(val)
		}
		return size
	}

	return len([]byte(fmt.Sprint(value)))
}
