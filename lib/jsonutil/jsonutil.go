package jsonutil

import (
	"encoding/json"
)

// SanitizePayload will take in a JSON string, and return a JSON string that has been sanitized (removed duplicate keys)
func SanitizePayload(val interface{}) (interface{}, error) {
	var jsonMap map[string]interface{}
	valString, isOk := val.(string)
	if !isOk {
		return val, nil
	}

	err := json.Unmarshal([]byte(valString), &jsonMap)
	if err == nil {
		valBytes, err := json.Marshal(jsonMap)
		if err == nil {
			return string(valBytes), nil
		}
	}

	return nil, err
}
