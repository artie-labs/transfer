package typing

import (
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

func BuildIntegerKind(optionalKind OptionalIntegerKind) KindDetails {
	return KindDetails{
		Kind:                Integer.Kind,
		OptionalIntegerKind: ToPtr(optionalKind),
	}
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
