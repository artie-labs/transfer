package debezium

import (
	"fmt"
	"slices"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	stringPrefix = "Struct{"
	stringSuffix = "}"
)

type PrimaryKeyPayload struct {
	Schema  FieldsObject   `json:"schema"`
	Payload map[string]any `json:"payload"`
}

func (p PrimaryKeyPayload) parseAndReturnPayload() (map[string]any, error) {
	if len(p.Schema.Fields) == 0 {
		return p.Payload, nil
	}

	retMap := make(map[string]any)
	for key, value := range p.Payload {
		idx := slices.IndexFunc(p.Schema.Fields, func(field Field) bool {
			return field.FieldName == key
		})

		if idx < 0 {
			retMap[key] = value
		} else {
			parsedValue, err := p.Schema.Fields[idx].ParseValue(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse primary key: %q: %w", key, err)
			}

			retMap[key] = parsedValue
		}
	}

	return retMap, nil
}

func ParsePartitionKey(key []byte, cdcKeyFormat string) (map[string]any, error) {
	switch cdcKeyFormat {
	case kafkalib.JSONKeyFmt:
		return parsePartitionKeyStruct(key)
	case kafkalib.StringKeyFmt:
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
func parsePartitionKeyString(keyBytes []byte) (map[string]any, error) {
	// Key will look like key: Struct{quarter_id=1,course_id=course1,student_id=1}
	if len(keyBytes) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	keyString := string(keyBytes)
	if len(stringPrefix+stringSuffix) >= len(keyString) {
		return nil, fmt.Errorf("key is too short")
	}

	if !(strings.HasPrefix(keyString, stringPrefix) && strings.HasSuffix(keyString, stringSuffix)) {
		return nil, fmt.Errorf("incorrect key structure")
	}

	retMap := make(map[string]any)
	parsedKeyString := keyString[len(stringPrefix) : len(keyString)-1]
	for _, kvPartString := range strings.Split(parsedKeyString, ",") {
		kvParts := strings.Split(kvPartString, "=")
		if len(kvParts) < 2 {
			return nil, fmt.Errorf("malformed key value pair: %q", kvPartString)
		}

		retMap[kvParts[0]] = strings.Join(kvParts[1:], "=")
	}
	// Skip this key.
	delete(retMap, constants.DebeziumTopicRoutingKey)
	return sanitizePayload(retMap), nil
}

func parsePartitionKeyStruct(keyBytes []byte) (map[string]any, error) {
	if len(keyBytes) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	var pkStruct map[string]any
	if err := json.Unmarshal(keyBytes, &pkStruct); err != nil {
		return nil, fmt.Errorf("failed to json unmarshal into map[string]any: %w", err)
	}

	if len(pkStruct) == 0 {
		return nil, fmt.Errorf("key is nil")
	}

	_, ok := pkStruct["payload"]
	if !ok {
		// pkStruct does not have schema enabled
		return sanitizePayload(pkStruct), nil
	}

	// If it does have a `payload` object, it should also have a schema object.
	var primaryKeyPayload PrimaryKeyPayload
	if err := json.Unmarshal(keyBytes, &primaryKeyPayload); err != nil {
		return nil, fmt.Errorf("failed to json unmarshal into PrimaryKeyPayload: %w", err)
	}

	keys, err := primaryKeyPayload.parseAndReturnPayload()
	if err != nil {
		return nil, err
	}

	delete(keys, constants.DebeziumTopicRoutingKey)
	return sanitizePayload(keys), nil
}

func sanitizePayload(retMap map[string]any) map[string]any {
	escapedRetMap := make(map[string]any)
	for key, value := range retMap {
		escapedRetMap[columns.EscapeName(key)] = value
	}

	return escapedRetMap
}
