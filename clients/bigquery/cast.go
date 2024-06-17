package bigquery

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// EncodeStructToJSONString takes a struct as either a string or Go object and encodes it into a JSON string.
// Structs from relational and Mongo are different.
// MongoDB will return the native objects back such as `map[string]any{"hello": "world"}`
// Relational will return a string representation of the struct such as `{"hello": "world"}`
func EncodeStructToJSONString(value any) (string, error) {
	if stringValue, isOk := value.(string); isOk {
		if strings.Contains(stringValue, constants.ToastUnavailableValuePlaceholder) {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
		}
		return stringValue, nil
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value: %w", err)
	}

	stringValue := string(bytes)
	if strings.Contains(stringValue, constants.ToastUnavailableValuePlaceholder) {
		// TODO: Remove this if we don't see it in the logs.
		slog.Error("encoded JSON value contains the toast unavailable value placeholder")
		return fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), nil
	}
	return stringValue, nil
}
