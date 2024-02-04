package postgresql

import (
	"github.com/artie-labs/transfer/lib/typing/values"

	"github.com/artie-labs/transfer/lib/typing/columns"
)

// CastColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func (s *Store) CastColValStaging(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (any, error) {
	if colVal == nil {
		return nil, nil
	}

	colValString, err := values.ToString(colVal, colKind, additionalDateFmts)
	if err != nil {
		return "", err
	}

	return colValString, nil
}
