package typing

import (
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Settings struct {
	AdditionalDateFormats []string `yaml:"additionalDateFormats"`
}

type KindDetails struct {
	Kind                   string
	ExtendedTimeDetails    *ext.NestedKind
	ExtendedDecimalDetails *decimal.Details

	// Optional kind details metadata
	OptionalStringPrecision *int32
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

func NewDecimalDetailsFromTemplate(details KindDetails, decimalDetails decimal.Details) KindDetails {
	if details.ExtendedDecimalDetails == nil {
		details.ExtendedDecimalDetails = &decimalDetails
	}

	return details
}

func NewKindDetailsFromTemplate(details KindDetails, extendedType ext.ExtendedTimeKindType) KindDetails {
	if details.ExtendedTimeDetails == nil {
		details.ExtendedTimeDetails = &ext.NestedKind{}
	}

	details.ExtendedTimeDetails.Type = extendedType
	return details
}
