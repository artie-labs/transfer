package size

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	//"unsafe"
)

// getRealSizeOf will encode the actual variable into bytes and then check the length
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal
// Lifted this from: https://stackoverflow.com/a/60508928
func getRealSizeOf(v interface{}) (int, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0, err
	}
	return b.Len(), nil
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
