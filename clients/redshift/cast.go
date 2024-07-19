package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

const maxRedshiftLength int32 = 65535

// replaceExceededValues - takes `colVal` any and `colKind` columns.Column and replaces the value with an empty string if it exceeds the max length.
// This currently only works for STRING and SUPER data types.
func replaceExceededValues(colVal string, colKind columns.Column) string {
	structOrString := colKind.KindDetails.Kind == typing.Struct.Kind || colKind.KindDetails.Kind == typing.String.Kind
	if structOrString {
		maxLength := maxRedshiftLength
		// If the customer has specified the maximum string precision, let's use that as the max length.
		if colKind.KindDetails.OptionalStringPrecision != nil {
			maxLength = *colKind.KindDetails.OptionalStringPrecision
		}

		if shouldReplace := int32(len(colVal)) > maxLength; shouldReplace {
			if colKind.KindDetails.Kind == typing.Struct.Kind {
				return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
			}

			return constants.ExceededValueMarker
		}
	}

	return colVal
}

// CastColValStaging - takes `colVal` any and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func (s *Store) CastColValStaging(colVal any, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		if colKind.KindDetails == typing.Struct {
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
