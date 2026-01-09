package converters

import (
	"encoding/base64"
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type Bytes struct{}

// Convert attempts to convert a value (type []byte, or string) to a slice of bytes.
// - If value is already a slice of bytes it will be directly returned.
// - If value is a string we will attempt to base64 decode it.
func (Bytes) Convert(value any) (any, error) {
	switch castedValue := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return castedValue, nil
	case string:
		if castedValue == constants.ToastUnavailableValuePlaceholder {
			return constants.ToastUnavailableValuePlaceholder, nil
		}

		data, err := base64.StdEncoding.DecodeString(castedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to base64 decode: %w", err)
		}

		if string(data) == constants.ToastUnavailableValuePlaceholder {
			return constants.ToastUnavailableValuePlaceholder, nil
		}

		return data, nil
	default:
		return nil, fmt.Errorf("expected []byte or string, got %T", value)
	}
}
