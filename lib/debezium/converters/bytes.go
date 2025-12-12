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
	// Defensive check for nil (should be handled upstream but adding for safety)
	if value == nil {
		return nil, nil
	}

	switch castedValue := value.(type) {
	case []byte:
		return castedValue, nil
	case string:
		// Handle empty strings as nil/empty bytes
		if castedValue == "" {
			return nil, nil
		}
		data, err := base64.StdEncoding.DecodeString(castedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("expected []byte or string, got %T", value)
	}
}
