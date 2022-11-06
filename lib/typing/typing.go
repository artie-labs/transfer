package typing

import (
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
)

var supportedDateTimeLayouts = []string{
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

func ParseValue(val interface{}) Kind {
	// Check if it's a number first.
	switch val.(type) {
	case nil:
		return Invalid
	case uint, int, uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return Integer
	case float32, float64:
		return Float
	case bool:
		return Boolean
	case string:
		valString := fmt.Sprint(val)
		// If it contains space or -, then we must check against date time.
		// This way, we don't penalize every string into going through this loop
		// In the future, we can have specific layout RFCs run depending on the char
		if strings.Contains(valString, " ") || strings.Contains(valString, "-") {
			for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
				_, err := time.Parse(supportedDateTimeLayout, valString)
				if err == nil {
					return DateTime
				}
			}
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
