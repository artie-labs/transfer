package typing

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing/decimal"
)

type OptionalIntegerKind int

const (
	NotSpecifiedKind OptionalIntegerKind = iota
	SmallIntegerKind
	IntegerKind
	BigIntegerKind
)

type KindType int

const (
	KindInvalid KindType = iota
	KindFloat
	KindInteger
	KindDecimal
	KindBoolean
	KindArray
	KindStruct
	KindString
	KindBytes
	KindDate
	KindTime
	KindTimestampNTZ
	KindTimestampTZ
	KindUUID
	KindInterval
)

func (k KindType) String() string {
	switch k {
	case KindInvalid:
		return "invalid"
	case KindFloat:
		return "float"
	case KindInteger:
		return "int"
	case KindDecimal:
		return "decimal"
	case KindBoolean:
		return "bool"
	case KindArray:
		return "array"
	case KindStruct:
		return "struct"
	case KindString:
		return "string"
	case KindBytes:
		return "bytes"
	case KindDate:
		return "date"
	case KindTime:
		return "time"
	case KindTimestampNTZ:
		return "timestamp_ntz"
	case KindTimestampTZ:
		return "timestamp_tz"
	case KindUUID:
		return "uuid"
	case KindInterval:
		return "interval"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// TODO: KindDetails should store the raw data type from the target table (if exists).
type KindDetails struct {
	Kind                   KindType
	ExtendedDecimalDetails *decimal.Details

	// Optional kind details metadata
	OptionalStringPrecision *int32
	OptionalIntegerKind     *OptionalIntegerKind
	// [OptionalArrayKind] - This is only populated for Postgres.
	OptionalArrayKind *KindDetails
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
		Kind: KindInvalid,
	}

	Float = KindDetails{
		Kind: KindFloat,
	}

	Integer = KindDetails{
		Kind:                KindInteger,
		OptionalIntegerKind: ToPtr(NotSpecifiedKind),
	}

	EDecimal = KindDetails{
		Kind: KindDecimal,
	}

	Boolean = KindDetails{
		Kind: KindBoolean,
	}

	Array = KindDetails{
		Kind: KindArray,
	}

	Struct = KindDetails{
		Kind: KindStruct,
	}

	String = KindDetails{
		Kind: KindString,
	}

	Bytes = KindDetails{
		Kind: KindBytes,
	}

	// Time data types
	Date = KindDetails{
		Kind: KindDate,
	}

	TimeKindDetails = KindDetails{
		Kind: KindTime,
	}

	TimestampNTZ = KindDetails{
		Kind: KindTimestampNTZ,
	}

	TimestampTZ = KindDetails{
		Kind: KindTimestampTZ,
	}

	// [UUID] - This is only populated for Postgres for now.
	UUID = KindDetails{
		Kind: KindUUID,
	}

	// [Interval] - This is only populated for Postgres for now.
	Interval = KindDetails{
		Kind: KindInterval,
	}
)

func NewDecimalDetailsFromTemplate(details KindDetails, decimalDetails decimal.Details) KindDetails {
	if details.ExtendedDecimalDetails == nil {
		details.ExtendedDecimalDetails = &decimalDetails
	}

	return details
}
