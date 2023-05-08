package size

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	//"unsafe"
)

// GetRealSizeOf will encode the actual variable into bytes and then check the length
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal
// Lifted this from: https://stackoverflow.com/a/60508928
func GetRealSizeOf(v interface{}) (int, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0, err
	}
	return b.Len(), nil
}

func CrossedThreshold(value interface{}, bytesLimit int) (bool, error) {
	valBytes, err := GetRealSizeOf(value)
	if err != nil {
		return false, err
	}

	return bytesLimit > valBytes, nil
}
