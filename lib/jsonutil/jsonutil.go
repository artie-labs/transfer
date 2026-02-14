package jsonutil

import jsoniter "github.com/json-iterator/go"

// useNumberJSON is a jsoniter API that is compatible with the standard library but with UseNumber enabled.
// This preserves precision for large integers (int64 values > 2^53) by decoding JSON numbers into
// [json.Number] (a string type) instead of float64 when the target type is [any]/[interface{}].
var useNumberJSON = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

// Unmarshal decodes JSON data into [v] using UseNumber to preserve integer precision.
// When unmarshalling into [any] or [map[string]any], JSON numbers will be represented as [json.Number]
// instead of float64, avoiding precision loss for large int64 values.
func Unmarshal(data []byte, v any) error {
	return useNumberJSON.Unmarshal(data, v)
}

func UnmarshalPayload(val string) (any, error) {
	// There are edge cases for when this may happen
	// Example: JSONB column in a table in Postgres where the table replica identity is set to `default` and it was a delete event.
	if val == "" {
		return "", nil
	}

	var obj any
	if err := Unmarshal([]byte(val), &obj); err != nil {
		return nil, err
	}

	return obj, nil
}
