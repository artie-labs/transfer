package typing

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"strings"
)

type Column struct {
	Name string
	Kind Kind
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
// srcKeyMissing - which key are missing from source that are present in target
// targKeyMissing - which key are missing from target that are present in source
func Diff(source map[string]Kind, target map[string]Kind) (srcKeyMissing []Column, targKeyMissing []Column) {
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

func CopyColMap(source map[string]Kind) map[string]Kind {
	retVal := make(map[string]Kind)
	for k, v := range source {
		retVal[strings.ToLower(k)] = v
	}

	return retVal
}
