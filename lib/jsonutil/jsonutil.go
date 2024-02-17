package jsonutil

import (
	"encoding/json"
)

// SanitizePayload will take in a JSON string, and return a JSON string that has been sanitized (removed duplicate keys)
func SanitizePayload(val interface{}) (interface{}, error) {
	valString, isOk := val.(string)
	if !isOk {
		return val, nil
	}

	var obj interface{}
	err := json.Unmarshal([]byte(valString), &obj)
	if err == nil {
		valBytes, err := json.Marshal(obj)
		if err == nil {
			return string(valBytes), nil
		}
	}

	return nil, err
}
