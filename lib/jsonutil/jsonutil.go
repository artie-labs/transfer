package jsonutil

import (
	"encoding/json"
	"fmt"
)

// SanitizePayload will take in a JSON string, and return a JSON string that has been sanitized (removed duplicate keys)
func SanitizePayload(val interface{}) (interface{}, error) {
	_, isOk := val.(map[string]interface{})
	if isOk {
		return val, nil
	}

	var jsonMap map[string]interface{}
	err := json.Unmarshal([]byte(fmt.Sprint(val)), &jsonMap)
	if err == nil {
		valBytes, err := json.Marshal(jsonMap)
		if err == nil {
			return string(valBytes), nil
		}
	}

	return nil, err
}
