package typing

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Kind string

// Summarized this from Snowflake + Reflect.
// In the future, we can support Geo objects.
const (
	Invalid  Kind = "invalid"
	Float    Kind = "float"
	Integer  Kind = "int"
	Boolean  Kind = "bool"
	Array    Kind = "array"
	Struct   Kind = "struct"
	DateTime Kind = "datetime"
	String   Kind = "string"
	ISO8601       = "2006-01-02T15:04:05-07:00"
)

var supportedDateTimeLayouts = []string{
	ISO8601,
	time.Layout,
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC3339,
}

// IsJSON - We also need to check if the string is a JSON string or not
// If it could be one, it will start with { and end with }.
// Once there, we will then check if it's a JSON string or not.
// This is an optimization since JSON string checking is expensive.
func IsJSON(str string) bool {
	if len(str) < 2 {
		// Not LTE 2 because {} is a valid JSON object.
		return false
	}

	// Shouldn't need to strings.TrimSpace(...).
	valStringChars := []rune(str)
	if string(valStringChars[0]) == "{" && string(valStringChars[len(valStringChars)-1]) == "}" {
		var js json.RawMessage
		return json.Unmarshal([]byte(str), &js) == nil
	}

	return false
}

func ParseDateTime(dtString string) (ts time.Time, err error) {
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		ts, err = time.Parse(supportedDateTimeLayout, dtString)
		if err == nil {
			return
		}
	}

	return
}

func ParseValue(val interface{}) Kind {
	// Check if it's a number first.
	switch val.(type) {
	case nil:
		return Invalid
	case uint, int, uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return Integer
	case float32, float64:
		// Integers will be parsed as Floats if they come from JSON
		// This is a limitation with JSON - https://github.com/golang/go/issues/56719
		// UNLESS Transfer is provided with a schema object, and we deliberately typecast the value to an integer
		// before calling ParseValue().
		return Float
	case bool:
		return Boolean
	case string:
		valString := fmt.Sprint(val)
		// If it contains space or -, then we must check against date time.
		// This way, we don't penalize every string into going through this loop
		// In the future, we can have specific layout RFCs run depending on the char
		if strings.Contains(valString, " ") || strings.Contains(valString, "-") {
			_, err := ParseDateTime(valString)
			if err == nil {
				return DateTime
			}
		}

		if IsJSON(valString) {
			return Struct
		}

		return String
	default:
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return Array
		} else if reflect.TypeOf(val).Kind() == reflect.Map {
			return Struct
		}

		break
	}

	return Invalid
}
