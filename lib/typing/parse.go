package typing

import (
	"fmt"
	"log/slog"
	"reflect"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ParseValue(_ Settings, key string, optionalSchema map[string]KindDetails, val any) KindDetails {
	if kindDetail, isOk := optionalSchema[key]; isOk {
		return kindDetail
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

		slog.Warn("Unhandled value, we returning Invalid for this type", slog.String("type", fmt.Sprintf("%T", val)), slog.Any("value", val))
	}

	return Invalid
}
