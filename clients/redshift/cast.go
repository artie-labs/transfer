package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/converters"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/artie-labs/transfer/lib/typing/values"
)

type Result struct {
	Value string
	// NewLength - If the value exceeded the maximum length, this will be the new length of the value.
	// This is only applicable if [expandStringPrecision] is enabled.
	NewLength int32
}

const (
	maxStringLength int32 = 65535
	maxSuperLength        = 16 * 1024 * 1024
)

func replaceExceededValues(colVal string, colKind typing.KindDetails, truncateExceededValue bool, expandStringPrecision bool) Result {
	switch colKind.Kind {
	case typing.Struct.Kind:
		// If the value is a JSON object, we will use [maxSuperLength], else we will use [maxStringLength]
		// Ref: https://docs.aws.amazon.com/redshift/latest/dg/limitations-super.html
		if typing.IsJSON(colVal) {
			if len(colVal) > maxSuperLength {
				fmt.Println("colVal", colVal)
				return Result{Value: fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)}
			}

			return Result{Value: colVal}
		}

		// Try again, but use [typing.String] instead.
		return replaceExceededValues(colVal, typing.String, truncateExceededValue, expandStringPrecision)
	case typing.String.Kind:
		maxLength := typing.DefaultValueFromPtr[int32](colKind.OptionalStringPrecision, maxStringLength)
		colValLength := int32(len(colVal))
		// If [expandStringPrecision] is enabled and the value is greater than the maximum length, and lte Redshift's max length.
		if expandStringPrecision && colValLength > maxLength && colValLength <= maxStringLength {
			return Result{Value: colVal, NewLength: colValLength}
		}

		if shouldReplace := colValLength > maxLength; shouldReplace {
			fmt.Println("colValLength", colValLength, "maxLength", maxLength, "colVal", colVal)
			if truncateExceededValue {
				return Result{Value: colVal[:maxLength]}
			} else {
				return Result{Value: constants.ExceededValueMarker}
			}
		}
	}

	return Result{Value: colVal}
}

func castColValStaging(colVal any, colKind typing.KindDetails, truncateExceededValue bool, expandStringPrecision bool) (Result, error) {
	if colVal == nil {
		if colKind == typing.Struct {
			// Returning empty here because if it's a struct, it will go through JSON PARSE and JSON_PARSE("") = null
			return Result{}, nil
		}

		// This matches the COPY clause for NULL terminator.
		return Result{Value: constants.NullValuePlaceholder}, nil
	}

	// Redshift only allows up to microsecond precision: https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html
	colValString, err := values.ToStringOpts(colVal, colKind, converters.GetStringConverterOpts{
		TimestampTZLayoutOverride:  ext.RFC3339MicroTZ,
		TimestampNTZLayoutOverride: ext.RFC3339MicroTZNoTZ,
	})

	if err != nil {
		return Result{}, err
	}

	// Checks for DDL overflow needs to be done at the end in case there are any conversions that need to be done.
	return replaceExceededValues(colValString, colKind, truncateExceededValue, expandStringPrecision), nil
}
