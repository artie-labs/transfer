package typing

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type KindDetails struct {
	Kind string

	ExtendedTimeDetails *NestedKind
}

const (
	ISO8601 = "2006-01-02T15:04:05-07:00"

	PostgresDateFormat = "2006-01-02"

	PostgresTimeFormat     = "15:04:05.999999-07" // microsecond precision
	AdditionalTimeFormat   = "15:04:05.999999Z07"
	PostgresTimeFormatNoTZ = "15:04:05.999999" // microsecond precision, used because certain destinations do not like `Time` types to specify tz locale
)

// Summarized this from Snowflake + Reflect.
// In the future, we can support Geo objects.
var (
	Invalid = KindDetails{
		Kind: "invalid",
	}

	Float = KindDetails{
		Kind: "float",
	}

	Integer = KindDetails{
		Kind: "int",
	}

	Boolean = KindDetails{
		Kind: "bool",
	}

	Array = KindDetails{
		Kind: "array",
	}

	Struct = KindDetails{
		Kind: "struct",
	}

	String = KindDetails{
		Kind: "string",
	}

	ETime = KindDetails{
		Kind: "extended_time",
	}
)

// TODO - Test.
func NewKindDetailsFromTemplate(details KindDetails, extendedType ExtendedTimeKindType) KindDetails {
	if details.ExtendedTimeDetails == nil {
		details.ExtendedTimeDetails = &NestedKind{}
	}

	details.ExtendedTimeDetails.Type = extendedType
	return details
}

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

var supportedDateFormats = []string{
	PostgresDateFormat,
}

var supportedTimeFormats = []string{
	PostgresTimeFormat,
	PostgresTimeFormatNoTZ,
	AdditionalTimeFormat,
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

// ParseExtendedDateTime will take a string and check if the string is of the following types:
// - Timestamp w/ timezone
// - Timestamp w/o timezone
// - Date
// - Time w/ timezone
// - Time w/o timezone
// It will then return an extended Time object from Transfer which allows us to build additional functionality
// on top of Golang's time.Time library by preserving original format and replaying to the destination without
// overlaying or mutating any format and timezone shifts.
func ParseExtendedDateTime(dtString string) (*ExtendedTime, error) {
	// Check all the timestamp formats
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		ts, err := time.Parse(supportedDateTimeLayout, dtString)
		if err == nil {
			return NewExtendedTime(ts, DateTimeKindType, supportedDateTimeLayout)
		}
	}

	// Now check dates
	for _, supportedDateFormat := range supportedDateFormats {
		date, err := time.Parse(supportedDateFormat, dtString)
		if err == nil {
			return NewExtendedTime(date, DateKindType, supportedDateFormat)
		}
	}

	// Now check time w/o TZ
	for _, supportedTimeFormat := range supportedTimeFormats {
		_time, err := time.Parse(supportedTimeFormat, dtString)
		if err == nil {
			return NewExtendedTime(_time, TimeKindType, supportedTimeFormat)
		}
	}

	// TODO: What about time w/ TZ?
	return nil, fmt.Errorf("dtString: %s is not supported", dtString)
}

func ParseValue(val interface{}) KindDetails {
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
		if strings.Contains(valString, ":") || strings.Contains(valString, "-") {
			extendedKind, err := ParseExtendedDateTime(valString)
			if err == nil {
				return KindDetails{
					Kind:                ETime.Kind,
					ExtendedTimeDetails: &extendedKind.NestedKind,
				}
			}
		}

		if IsJSON(valString) {
			return Struct
		}

		return String
	default:
		// Check if the val is one of our custom-types
		extendedKind, isOk := val.(*ExtendedTime)
		if isOk {
			return KindDetails{
				Kind:                ETime.Kind,
				ExtendedTimeDetails: &extendedKind.NestedKind,
			}
		}

		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return Array
		} else if reflect.TypeOf(val).Kind() == reflect.Map {
			return Struct
		}

		break
	}

	return Invalid
}
