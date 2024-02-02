package redshift

import (
	"github.com/artie-labs/transfer/lib/typing/values"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

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

	return colValString, nil
}
