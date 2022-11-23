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

func TestNotEmpty(t *testing.T) {
	notEmptyList := []string{
		"aaa",
		"foo",
		"bar",
	}

	assert.False(t, Empty(notEmptyList))

	notEmptyList = append(notEmptyList, "")
	assert.True(t, Empty(notEmptyList))
}
