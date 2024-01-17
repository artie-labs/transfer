package jsonutil

import (
	"encoding/json"
	"fmt"
)

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
			return valBytes, nil
		}
	}

	return nil, err
}
