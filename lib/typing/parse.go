package typing

import (
	"reflect"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// TODO: Separate PR to remove `settings` completely. Not doing it first pass because it'll generate a big diff.
func ParseValue(_ Settings, key string, optionalSchema map[string]KindDetails, val any) KindDetails {
	if len(optionalSchema) > 0 {
		// If the column exists in the schema, let's early exit.
		if kindDetail, isOk := optionalSchema[key]; isOk {
			// If the schema exists, use it as sot.
			if val != nil && (kindDetail.Kind == ETime.Kind || kindDetail.Kind == EDecimal.Kind) {
				// If the data type is either `ETime` or `EDecimal` and the value exists, we will not early exit
				// We are not skipping so that we are able to get the exact layout specified at the row level to preserve:
				// 1. Layout for time / date / timestamps
				// 2. Precision and scale for numeric values
				return parseValue(val)
			}

			return kindDetail
		}
	}

	return parseValue(val)
}

func parseValue(val any) KindDetails {
	switch convertedVal := val.(type) {
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
		if IsJSON(convertedVal) {
			return Struct
		}

		return String
	case *decimal.Decimal:
		extendedDetails := convertedVal.Details()
		return KindDetails{
			Kind:                   EDecimal.Kind,
			ExtendedDecimalDetails: &extendedDetails,
		}
	case *ext.ExtendedTime:
		return KindDetails{
			Kind:                ETime.Kind,
			ExtendedTimeDetails: &convertedVal.NestedKind,
		}
	default:
		// Check if the val is one of our custom-types
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return Array
		} else if reflect.TypeOf(val).Kind() == reflect.Map {
			return Struct
		}
	}

	return Invalid
}
