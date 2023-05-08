package size

import (
	"fmt"
)

// getRealSizeOf will encode the actual variable into bytes and then check the length (approx) by using string encoding.
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal.
// We also chose not to use gob.NewEncoder because it does not work for all data types and had a huge computational overhead.
func getRealSizeOf(v interface{}) (int, error) {
	valString := fmt.Sprint(v)

	return len([]byte(valString)), nil
}

func CrossedThreshold(value interface{}, kbLimit int) (bool, error) {
	valBytes, err := getRealSizeOf(value)
	if err != nil {
		return false, err
	}

	return valBytes > kbToBytes(kbLimit), nil
}

func kbToBytes(kbNum int) int {
	return kbNum * 1024
}
