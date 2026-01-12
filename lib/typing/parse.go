package typing

import (
	"fmt"
	"reflect"
	"time"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// MustParseValue - panics if the value cannot be parsed. This is used only for tests.
func MustParseValue(key string, optionalSchema map[string]KindDetails, val any) KindDetails {
	kindDetail, err := ParseValue(key, optionalSchema, val)
	if err != nil {
		panic(err)
	}

	return kindDetail
}

func ParseValue(key string, optionalSchema map[string]KindDetails, val any) (KindDetails, error) {
	if kindDetail, ok := optionalSchema[key]; ok {
		return kindDetail, nil
	}

	switch convertedVal := val.(type) {
	case nil:
		return Invalid, nil
	case uint, int, uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return Integer, nil
	case float32, float64:
		// Integers will be parsed as Floats if they come from JSON
		// This is a limitation with JSON - https://github.com/golang/go/issues/56719
		// UNLESS Transfer is provided with a schema object, and we deliberately typecast the value to an integer
		// before calling ParseValue().
		return Float, nil
	case bool:
		return Boolean, nil
	case string:
		if IsJSON(convertedVal) {
			return Struct, nil
		}

		return String, nil
	case *decimal.Decimal:
		extendedDetails := convertedVal.Details()
		return KindDetails{
			Kind:                   EDecimal.Kind,
			ExtendedDecimalDetails: &extendedDetails,
		}, nil
	case time.Time:
		return TimestampTZ, nil
	case ext.Time:
		return TimeKindDetails, nil
	default:
		// Check if the val is one of our custom-types
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return Array, nil
		} else if reflect.TypeOf(val).Kind() == reflect.Map {
			return Struct, nil
		}
	}

	return Invalid, fmt.Errorf("unknown type: %T, value: %v", val, val)
}
