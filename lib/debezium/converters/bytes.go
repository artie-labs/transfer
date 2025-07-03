package converters

import (
	"encoding/base64"
	"fmt"
)

type Bytes struct{}

// Convert attempts to convert a value (type []byte, or string) to a slice of bytes.
// - If value is already a slice of bytes it will be directly returned.
// - If value is a string we will attempt to base64 decode it.
func (Bytes) Convert(value any) (any, error) {
	if bytes, ok := value.([]byte); ok {
		return bytes, nil
	}

	if stringValue, ok := value.(string); ok {
		data, err := base64.StdEncoding.DecodeString(stringValue)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode: %w", err)
		}
		return data, nil
	}

	return nil, fmt.Errorf("expected []byte or string, got %T", value)
}
