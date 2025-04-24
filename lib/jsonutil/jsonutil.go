package jsonutil

import (
	"encoding/json"
)

// ParsePayload will take in a JSON string, and return a JSON string that has been sanitized (removed duplicate keys)
func ParsePayload(val string) (any, error) {
	// There are edge cases for when this may happen
	// Example: JSONB column in a table in Postgres where the table replica identity is set to `default` and it was a delete event.
	if val == "" {
		return "", nil
	}

	var obj any
	if err := json.Unmarshal([]byte(val), &obj); err != nil {
		return nil, err
	}

	return obj, nil
}
