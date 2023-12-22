package size

import "fmt"

// GetApproxSize will encode the actual variable into bytes and then check the length (approx) by using string encoding.
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal.
// We also chose not to use gob.NewEncoder because it does not work for all data types and had a huge computational overhead.
// Another bonus is that there is no possible way for this to error out.
func GetApproxSize(v map[string]interface{}) int {
	var size int
	for key, value := range v {
		size += len(key)                      // Size of the key
		size += approximateSizeOfValue(value) // Size of the value
	}
	return size
}

func approximateSizeOfValue(value interface{}) int {
	switch v := value.(type) {
	case string:
		return len(v)
	case bool:
		return 1 // Size of a boolean
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr, float32, float64:
		return 8 // Approximation for floating-point types
	case complex64, complex128:
		return 16 // Approximation for complex types
	case map[string]interface{}:
		return GetApproxSize(v) // Recursive call for nested maps
	case []byte:
		return len(v) // Size of byte slice
	}

	return len([]byte(fmt.Sprint(value)))
}
