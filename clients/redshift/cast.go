package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

const (
	maxRedshiftVarCharLen = 65535
	maxRedshiftSuperLen   = 1 * 1024 * 1024 // 1 MB
)

// replaceExceededValues - takes `colVal` interface{} and `colKind` columns.Column and replaces the value with an empty string if it exceeds the max length.
// This currently only works for STRING and SUPER data types.
func replaceExceededValues(colVal string, colKind columns.Column) string {
	numOfChars := len(colVal)
	switch colKind.KindDetails.Kind {
	// Assuming this corresponds to SUPER type in Redshift
	case typing.Struct.Kind:
		shouldReplace := numOfChars > maxRedshiftSuperLen
		potentiallyReplace := numOfChars > maxRedshiftVarCharLen

		// If the data value is a string, there's still a 2^16 character limit despite it being a SUPER data type.
		// https://docs.aws.amazon.com/redshift/latest/dg/limitations-super.html
		if !shouldReplace && potentiallyReplace && !typing.IsJSON(colVal) {
			shouldReplace = true
		}

		if shouldReplace {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
		}
	case typing.String.Kind:
		if numOfChars > maxRedshiftVarCharLen {
			return constants.ExceededValueMarker
		}
	}

	return colVal
}

// CastColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func (s *Store) CastColValStaging(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (string, error) {
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
	if s.skipLgCols {
		colValString = replaceExceededValues(colValString, colKind)
	}

	return colValString, nil
}
