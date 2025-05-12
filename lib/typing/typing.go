package typing

import (
	"encoding/json"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
)

type OptionalIntegerKind int

const (
	NotSpecifiedKind OptionalIntegerKind = iota
	SmallIntegerKind
	IntegerKind
	BigIntegerKind
)

// TODO: KindDetails should store the raw data type from the target table (if exists).
type KindDetails struct {
	Kind                   string
	ExtendedDecimalDetails *decimal.Details

	// Optional kind details metadata
	OptionalStringPrecision *int32
	OptionalIntegerKind     *OptionalIntegerKind
}

func (k KindDetails) DecimalDetailsNotSet() bool {
	return k.ExtendedDecimalDetails == nil || k.ExtendedDecimalDetails.NotSet()
}

var (
	Invalid = KindDetails{
		Kind: "invalid",
	}

	Float = KindDetails{
		Kind: "float",
	}

	Integer = KindDetails{
		Kind:                "int",
		OptionalIntegerKind: ToPtr(NotSpecifiedKind),
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

	// Time data types
	Date = KindDetails{
		Kind: "date",
	}

	Time = KindDetails{
		Kind: "time",
	}

	TimestampNTZ = KindDetails{
		Kind: "timestamp_ntz",
	}

	TimestampTZ = KindDetails{
		Kind: "timestamp_tz",
	}
)

func NewDecimalDetailsFromTemplate(details KindDetails, decimalDetails decimal.Details) KindDetails {
	if details.ExtendedDecimalDetails == nil {
		details.ExtendedDecimalDetails = &decimalDetails
	}

	return details
}

// IsJSON - We also need to check if the string is a JSON string or not
// If it could be one, it will start with { and end with }.
// Once there, we will then check if it's a JSON string or not.
// This is an optimization since JSON string checking is expensive.
func IsJSON(str string) bool {
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return false
	}

	firstChar := str[0]
	if firstChar != '{' && firstChar != '[' {
		return false
	}
	return json.Valid([]byte(str))
}
