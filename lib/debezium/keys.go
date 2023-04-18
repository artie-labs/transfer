package debezium

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	KeyFormatJSON   = "org.apache.kafka.connect.json.JsonConverter"
	KeyFormatString = "org.apache.kafka.connect.storage.StringConverter"

	stringPrefix = "Struct{"
	stringSuffix = "}"
)

func ParsePartitionKey(key []byte, cdcKeyFormat string) (map[string]interface{}, error) {
	switch cdcKeyFormat {
	case KeyFormatJSON:
		return parsePartitionKeyStruct(key)
	case KeyFormatString:
		return parsePartitionKeyString(key)

	}
	return nil, fmt.Errorf("format: %s is not supported", cdcKeyFormat)
}

// parsePartitionKeyString is used to parse the partition key when it is getting emitted in the string format.
// This is not the recommended approach because through serializing a Struct into a string notation, the operation is buggy and potentially irreversible.
// Kafka's string serialization will emit the message to look like: Struct{k=v,k1=v1}
// However, if the k or v has `,` or `=` within it, it is not escaped and thus difficult to delineate between a separator or a continuation of the column or value.
// In the case where there are multiple `=`, we will use the first one to separate between the key and value.
// TL;DR - Use `org.apache.kafka.connect.json.JsonConverter` over `org.apache.kafka.connect.storage.StringConverter`
func parsePartitionKeyString(key []byte) (map[string]interface{}, error) {
	// Key will look like key: Struct{quarter_id=1,course_id=course1,student_id=1}
	if len(key) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	keyString := string(key)
	if len("Struct{}") >= len(keyString) {
		return nil, fmt.Errorf("key is too short")
	}

	if !(strings.HasPrefix(keyString, stringPrefix) && strings.HasSuffix(keyString, stringSuffix)) {
		return nil, fmt.Errorf("incorrect key structure")
	}

	retMap := make(map[string]interface{})
	parsedKeyString := keyString[len(stringPrefix) : len(keyString)-1]
	for _, kvPartString := range strings.Split(parsedKeyString, ",") {
		kvParts := strings.Split(kvPartString, "=")
		if len(kvParts) < 2 {
			return nil, fmt.Errorf("malformed key value pair: %s", kvPartString)
		}

		retMap[kvParts[0]] = strings.Join(kvParts[1:], "=")
	}

	return retMap, nil
}

func parsePartitionKeyStruct(key []byte) (map[string]interface{}, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	var pkStruct map[string]interface{}
	err := json.Unmarshal(key, &pkStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to json unmarshal, error: %v", err)
	}

	if len(pkStruct) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	_, isOk := pkStruct["payload"]
	if !isOk {
		// pkStruct does not have schema enabled
		return pkStruct, nil
	}

	pkStruct, isOk = pkStruct["payload"].(map[string]interface{})
	if !isOk {
		return nil, fmt.Errorf("key object is malformated")
	}

	return pkStruct, nil
}
