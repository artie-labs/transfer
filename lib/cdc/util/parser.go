package util

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Struct{quarter_id=1,course_id=course1,student_id=1}
// ParseStringKey expects the key format to look like Struct{id=47}
func ParseStringKey(key []byte) (pkName string, pkValue interface{}, err error) {
	if len(key) == 0 {
		err = fmt.Errorf("key is nil")
		return
	}

	keyString := string(key)
	if len(keyString) < 8 {
		return "", "",
			fmt.Errorf("key length too short, actual: %v, key: %s", len(keyString), keyString)
	}

	// Strip out the leading Struct{ and trailing }
	pkParts := strings.Split(keyString[7:len(keyString)-1], "=")
	if len(pkParts) != 2 {
		return "", "", fmt.Errorf("key length incorrect, actual: %v, key: %s", len(keyString), keyString)
	}

	return pkParts[0], pkParts[1], nil
}

func ParseJSONKey(key []byte) (pkName string, pkValue interface{}, err error) {
	if len(key) == 0 {
		err = fmt.Errorf("key is nil")
		return
	}

	var pkStruct map[string]interface{}
	err = json.Unmarshal(key, &pkStruct)
	if err != nil {
		return
	}

	_, isOk := pkStruct["payload"]
	if isOk {
		var castOk bool
		// strip the schema and focus in on payload
		pkStruct, castOk = pkStruct["payload"].(map[string]interface{})
		if !castOk {
			return "", "", fmt.Errorf("key object is malformated")
		}
	}

	// Given that this is the format, we will only have 1 key in here.
	for k, v := range pkStruct {
		pkName = k
		pkValue = v
		break
	}

	return
}
