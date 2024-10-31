package typing

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type OptionalIntegerKind int

const (
	NotSpecifiedKind OptionalIntegerKind = iota
	SmallIntegerKind
	IntegerKind
	BigIntegerKind
)

type KindDetails struct {
	Kind                   string
	ExtendedTimeDetails    *ext.NestedKind
	ExtendedDecimalDetails *decimal.Details

	// Optional kind details metadata
	OptionalStringPrecision *int32
	OptionalIntegerKind     *OptionalIntegerKind
}

func (k *KindDetails) EnsureExtendedTimeDetails() error {
	if k.ExtendedTimeDetails == nil {
		return fmt.Errorf("extended time details is not set")
	}

	if k.ExtendedTimeDetails.Format == "" {
		return fmt.Errorf("extended time details format is not set")
	}

	return nil
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

	TimestampNTZ = KindDetails{
		Kind: "timestamp_ntz",
	}

	TimestampTZ = KindDetails{
		Kind: "timestamp_tz",
	}

	ETime = KindDetails{
		Kind: "extended_time",
	}
)

func NewDecimalDetailsFromTemplate(details KindDetails, decimalDetails decimal.Details) KindDetails {
	if details.ExtendedDecimalDetails == nil {
		details.ExtendedDecimalDetails = &decimalDetails
	}

	return details
}

// MustNewExtendedTimeDetails - calls NewExtendedTimeDetails and panics if there is an error returned. This is used for tests.
func MustNewExtendedTimeDetails(details KindDetails, extendedType ext.ExtendedTimeKindType, optionalFormat string) KindDetails {
	nestedKind, err := NewExtendedTimeDetails(details, extendedType, optionalFormat)
	if err != nil {
		panic(err)
	}

	return nestedKind
}

func NewExtendedTimeDetails(details KindDetails, extendedType ext.ExtendedTimeKindType, optionalFormat string) (KindDetails, error) {
	nestedKind, err := ext.NewNestedKind(extendedType, optionalFormat)
	if err != nil {
		return Invalid, err
	}

	details.ExtendedTimeDetails = &nestedKind
	return details, nil
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
