package jsonutil

import (
	"encoding/json"
	"fmt"
)

// SanitizePayload will take in a JSON string, and return a JSON string that has been sanitized (removed duplicate keys)
func SanitizePayload(val any) (any, error) {
	valString, isOk := val.(string)
	if !isOk {
		return val, fmt.Errorf("expected string, got: %T", val)
	}

	// There are edge cases for when this may happen
	// Example: JSONB column in a table in Postgres where the table replica identity is set to `default` and it was a delete event.
	if valString == "" {
		return "", nil
	}

	var obj any
	if err := json.Unmarshal([]byte(valString), &obj); err != nil {
		return nil, err
	}

	valBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	return string(valBytes), nil
}
