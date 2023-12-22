package size

import "fmt"

// GetApproxSize will encode the actual variable into bytes and then check the length (approx) by using string encoding.
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal.
// We also chose not to use gob.NewEncoder because it does not work for all data types and had a huge computational overhead.
// Another bonus is that there is no possible way for this to error out.
func GetApproxSize(v map[string]interface{}) int {
	var size int
	for _, value := range v {
		size += approximateSizeOfValue(value)
	}
	return size
}

func approximateSizeOfValue(value interface{}) int {
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
		return 16 // Approximation for complex types
	case map[string]interface{}:
		return GetApproxSize(v) // Recursive call for nested maps
	}

	return len([]byte(fmt.Sprint(value)))
}
