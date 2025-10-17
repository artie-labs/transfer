package maputil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortedStringsMap(t *testing.T) {
	s := NewSortedStringsMap[int]()
	s.Add("foo", 1)
	s.Add("bar", 2)
	s.Add("baz", 3)
	assert.Equal(t, []string{"bar", "baz", "foo"}, s.Keys())

	// Testing all.
	var keys []string
	for key := range s.All() {
		keys = append(keys, key)
	}
	assert.Equal(t, []string{"bar", "baz", "foo"}, keys)
}
