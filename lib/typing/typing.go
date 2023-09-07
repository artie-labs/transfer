package typing

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type KindDetails struct {
	Kind                   string
	ExtendedTimeDetails    *ext.NestedKind
	ExtendedDecimalDetails *decimal.Decimal
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
	if len(str) < 2 {
		return false
	}

	valStringChars := []rune(strings.TrimSpace(str))
	firstChar := string(valStringChars[0])
	lastChar := string(valStringChars[len(valStringChars)-1])

	if (firstChar == "{" && lastChar == "}") || (firstChar == "[" && lastChar == "]") {
		var js json.RawMessage
		return json.Unmarshal([]byte(str), &js) == nil
	}

	return false
}

func ParseValue(ctx context.Context, key string, optionalSchema map[string]KindDetails, val interface{}) KindDetails {
	cfg := config.FromContext(ctx).Config
	if val == nil && !cfg.SharedTransferConfig.CreateAllColumnsIfAvailable {
		// If the value is nil and `createAllColumnsIfAvailable` = false, then return `Invalid
		return Invalid
	}

	if len(optionalSchema) > 0 {
		// If the column exists in the schema, let's early exit.
		if kindDetail, isOk := optionalSchema[key]; isOk {
			// If the schema exists, use it as sot.
			if val != nil && (kindDetail.Kind == ETime.Kind || kindDetail.Kind == EDecimal.Kind) {
				// If the data type is either `ETime` or `EDecimal` and the value exists, we will not early exit
				// We are not skipping so that we are able to get the exact layout specified at the row level to preserve:
				// 1. Layout for time / date / timestamps
				// 2. Precision and scale for numeric values
				return ParseValue(ctx, key, nil, val)
			}

			return kindDetail
		}
	}

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
			extendedKind, err := ext.ParseExtendedDateTime(ctx, valString)
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

	case *decimal.Decimal:
		extendedKind, isOk := val.(*decimal.Decimal)
		if isOk {
			return KindDetails{
				Kind:                   EDecimal.Kind,
				ExtendedDecimalDetails: extendedKind,
			}
		}
	case *ext.ExtendedTime:
		extendedKind, isOk := val.(*ext.ExtendedTime)
		if isOk {
			return KindDetails{
				Kind:                ETime.Kind,
				ExtendedTimeDetails: &extendedKind.NestedKind,
			}
		}
	default:
		// Check if the val is one of our custom-types
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return Array
		} else if reflect.TypeOf(val).Kind() == reflect.Map {
			return Struct
		}

		break
	}

	return Invalid
}

func KindToDWHType(kd KindDetails, dwh constants.DestinationKind) string {
	switch dwh {
	case constants.Snowflake, constants.SnowflakeStages:
		return kindToSnowflake(kd)
	case constants.BigQuery:
		return kindToBigQuery(kd)
	case constants.Redshift:
		return kindToRedShift(kd)
	}

	return ""
}
