package array

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringsJoinAddPrefix(t *testing.T) {
	foo := []string{
		"abc",
		"def",
		"ggg",
	}

	assert.Equal(t, StringsJoinAddPrefix(foo, ", ", "ARTIE"), "ARTIEabc, ARTIEdef, ARTIEggg")
}
