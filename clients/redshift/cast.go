package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

const maxRedshiftLength int32 = 65535

// replaceExceededValues - takes `colVal` any and `colKind` columns.Column and replaces the value with an empty string if it exceeds the max length.
// This currently only works for STRING and SUPER data types.
func replaceExceededValues(colVal string, colKind typing.KindDetails) string {
	structOrString := colKind.Kind == typing.Struct.Kind || colKind.Kind == typing.String.Kind
	if structOrString {
		maxLength := maxRedshiftLength
		// If the customer has specified the maximum string precision, let's use that as the max length.
		if colKind.OptionalStringPrecision != nil {
			maxLength = *colKind.OptionalStringPrecision
		}

		if shouldReplace := int32(len(colVal)) > maxLength; shouldReplace {
			if colKind.Kind == typing.Struct.Kind {
				return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
			}

			return constants.ExceededValueMarker
		}
	}

	return colVal
}

func castColValStaging(colVal any, colKind typing.KindDetails, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		if colKind == typing.Struct {
			// Returning empty here because if it's a struct, it will go through JSON PARSE and JSON_PARSE("") = null
			return "", nil
		}

		// This matches the COPY clause for NULL terminator.
		return `\N`, nil
	}

	colValString, err := values.ToString(colVal, colKind, additionalDateFmts)
	if err != nil {
		return "", err
	}

	// Checks for DDL overflow needs to be done at the end in case there are any conversions that need to be done.
	return replaceExceededValues(colValString, colKind), nil
}
