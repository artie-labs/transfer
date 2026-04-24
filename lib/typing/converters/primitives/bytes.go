package primitives

import (
	"encoding/json"
	"fmt"
)

func AsBytes(value any) ([]byte, error) {
	switch castedValue := value.(type) {
	case []byte:
		return castedValue, nil
	case string:
		return []byte(castedValue), nil
	default:
		bytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %T to []byte: %w", value, err)
		}

		return bytes, nil
	}
}
