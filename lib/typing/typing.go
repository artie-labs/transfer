package typing

import (
	"encoding/json"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Settings struct {
	AdditionalDateFormats []string `yaml:"additionalDateFormats"`

	// CreateAllColumnsIfAvailable - If true, we will create all columns if the metadata is available regardless of
	// whether we have a value from the column. This will also bypass our Typing library.
	// This only works for data sources with a schema such as Postgres and MySQL
	CreateAllColumnsIfAvailable bool `yaml:"createAllColumnsIfAvailable"`
}

type KindDetails struct {
	Kind                   string
	ExtendedTimeDetails    *ext.NestedKind
	ExtendedDecimalDetails *decimal.Decimal

	// Optional kind details metadata
	OptionalStringPrecision *int
}

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

	EDecimal = KindDetails{
		Kind: "decimal",
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

func NewKindDetailsFromTemplate(details KindDetails, extendedType ext.ExtendedTimeKindType) KindDetails {
	if details.ExtendedTimeDetails == nil {
		details.ExtendedTimeDetails = &ext.NestedKind{}
	}

	details.ExtendedTimeDetails.Type = extendedType
	return details
}

// IsJSON - We also need to check if the string is a JSON string or not
// If it could be one, it will start with { and end with }.
// Once there, we will then check if it's a JSON string or not.
// This is an optimization since JSON string checking is expensive.
func IsJSON(str string) bool {
	str = strings.TrimSpace(str)
	if len(str) < 2 {
		return false
	}

	valStringChars := []rune(str)
	firstChar := string(valStringChars[0])
	lastChar := string(valStringChars[len(valStringChars)-1])

	if (firstChar == "{" && lastChar == "}") || (firstChar == "[" && lastChar == "]") {
		var js json.RawMessage
		return json.Unmarshal([]byte(str), &js) == nil
	}

	return false
}
