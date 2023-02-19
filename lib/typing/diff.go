package typing

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type Column struct {
	Name string
	Kind KindDetails
}

// shouldSkipColumn takes the `colName` and `softDelete` and will return whether we should skip this column when calculating the diff.
func shouldSkipColumn(colName string, softDelete bool) bool {
	if colName == constants.DeleteColumnMarker && softDelete {
		// We need this column to be created if soft deletion is turned on.
		return false
	}

	if strings.Contains(colName, constants.ArtiePrefix) {
		return true
	}

	return false
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
// The other argument `softDelete` is used on whether we should ignore Artie's soft delete column.
// srcKeyMissing - which key are missing from source that are present in target
// targKeyMissing - which key are missing from target that are present in source
func Diff(source map[string]KindDetails, target map[string]KindDetails, softDelete bool) (srcKeyMissing []Column, targKeyMissing []Column) {
	src := CopyColMap(source)
	targ := CopyColMap(target)

	for key := range src {
		_, isOk := targ[key]
		if isOk {
			delete(src, key)
			delete(targ, key)
		}
	}

	for name, kind := range src {
		if shouldSkipColumn(name, softDelete) {
			continue
		}

		targKeyMissing = append(targKeyMissing, Column{
			Name: name,
			Kind: kind,
		})
	}

	for name, kind := range targ {
		if shouldSkipColumn(name, softDelete) {
			continue
		}

		srcKeyMissing = append(srcKeyMissing, Column{
			Name: name,
			Kind: kind,
		})
	}

	return
}

func CopyColMap(source map[string]KindDetails) map[string]KindDetails {
	retVal := make(map[string]KindDetails)
	for k, v := range source {
		retVal[strings.ToLower(k)] = v
	}

	return retVal
}
