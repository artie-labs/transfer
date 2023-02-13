package typing

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type Column struct {
	Name string
	Kind KindDetails
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
// srcKeyMissing - which key are missing from source that are present in target
// targKeyMissing - which key are missing from target that are present in source
func Diff(source map[string]KindDetails, target map[string]KindDetails) (srcKeyMissing []Column, targKeyMissing []Column) {
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
		if strings.Contains(name, constants.ArtiePrefix) {
			// Ignore artie metadata
			continue
		}

		targKeyMissing = append(targKeyMissing, Column{
			Name: name,
			Kind: kind,
		})
	}

	for name, kind := range targ {
		if strings.Contains(name, constants.ArtiePrefix) {
			// Ignore artie metadata
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
