package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

const maxRedshiftLength int32 = 65535

func canIncreasePrecision(colKind typing.KindDetails, valueLength int32) bool {
	if colKind.Kind == typing.String.Kind && colKind.OptionalStringPrecision != nil {
		return maxRedshiftLength > *colKind.OptionalStringPrecision && valueLength <= maxRedshiftLength
	}

	return false
}

// replaceExceededValues replaces the value with a marker if it exceeds the maximum length
// Returns the value and boolean indicating whether the column should be increased or not.
func replaceExceededValues(colVal string, colKind typing.KindDetails, truncateExceededValue bool, increaseStringPrecision bool) (string, bool) {
	if colKind.Kind == typing.Struct.Kind || colKind.Kind == typing.String.Kind {
		maxLength := maxRedshiftLength
		// If the customer has specified the maximum string precision, let's use that as the max length.
		if colKind.OptionalStringPrecision != nil {
			maxLength = *colKind.OptionalStringPrecision
		}

		colValLength := int32(len(colVal))
		if shouldReplace := colValLength > maxLength; shouldReplace {
			if colKind.Kind == typing.Struct.Kind {
				return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), false
			}

			if increaseStringPrecision && canIncreasePrecision(colKind, colValLength) {
				return colVal, true
			}

			if truncateExceededValue {
				return colVal[:maxLength], false
			} else {
				return constants.ExceededValueMarker, false
			}
		}
	}

	return colVal, false
}

func castColValStaging(colVal any, colKind typing.KindDetails, truncateExceededValue bool, increaseStringPrecision bool) (string, bool, error) {
	if colVal == nil {
		if colKind == typing.Struct {
			// Returning empty here because if it's a struct, it will go through JSON PARSE and JSON_PARSE("") = null
			return "", false, nil
		}

		// This matches the COPY clause for NULL terminator.
		return `\N`, false, nil
	}

	colValString, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", false, err
	}

	// Checks for DDL overflow needs to be done at the end in case there are any conversions that need to be done.

	colValue, shouldIncreaseColumn := replaceExceededValues(colValString, colKind, truncateExceededValue, increaseStringPrecision)
	return colValue, shouldIncreaseColumn, nil
}
