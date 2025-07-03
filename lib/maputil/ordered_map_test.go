package maputil

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedMap(t *testing.T) {
	{
		// case sensitive
		o := NewOrderedMap[int](true)
		o.Add("foo", 1)
		o.Add("bar", 2)
		o.Add("baz", 3)

		{
			// Query for FOO
			foo, ok := o.Get("foo")
			assert.True(t, ok)
			assert.Equal(t, 1, foo)

			_, ok = o.Get("FOO")
			assert.False(t, ok)
		}

		bar, ok := o.Get("bar")
		assert.True(t, ok)
		assert.Equal(t, 2, bar)

		baz, ok := o.Get("baz")
		assert.True(t, ok)
		assert.Equal(t, 3, baz)

		_, ok = o.Get("qux")
		assert.False(t, ok)

		// Try removing a non-existent entry
		assert.False(t, o.Remove("this does not exist"))

		assert.True(t, o.Remove("bar"))
		// Now try to remove it again
		assert.False(t, o.Remove("bar"))
		_, ok = o.Get("bar")
		assert.False(t, ok)
		assert.Len(t, o.Keys(), 2)

		for _, expectedKey := range []string{"foo", "baz"} {
			var found bool
			for _, key := range o.Keys() {
				if key == expectedKey {
					found = true
					break
				}
			}

			assert.True(t, found, "expected key %s not found", expectedKey)
		}
	}
	{
		// case insensitive
		o := NewOrderedMap[int](false)
		o.Add("foo", 1)
		o.Add("bar", 2)
		o.Add("BAZ", 3)

		foo, ok := o.Get("FOO")
		assert.True(t, ok)
		assert.Equal(t, 1, foo)

		bar, ok := o.Get("BAR")
		assert.True(t, ok)
		assert.Equal(t, 2, bar)

		baz, ok := o.Get("baz")
		assert.True(t, ok)
		assert.Equal(t, 3, baz)

		// Try removing a non-existent entry
		assert.False(t, o.Remove("this does not exist"))

		assert.True(t, o.Remove("baR"))
		// Now try to remove it again
		assert.False(t, o.Remove("baR"))
		_, ok = o.Get("bar")
		assert.False(t, ok)

		for _, expectedKey := range []string{"foo", "baz"} {
			var found bool
			for _, key := range o.Keys() {
				if key == expectedKey {
					found = true
					break
				}
			}

			assert.True(t, found, "expected key %s not found", expectedKey)
		}
	}
	{
		// Create a new ordered map, add a bunch of same keys and make sure `keys` is unique
		o := NewOrderedMap[int](true)

		for i := range 100 {
			o.Add("foo", i)
		}

		assert.Len(t, o.Keys(), 1)
		foo, ok := o.Get("foo")
		assert.True(t, ok)
		assert.Equal(t, 99, foo)
	}
}

func TestOrderedMap_All(t *testing.T) {
	o := NewOrderedMap[int](true)
	assert.Equal(t, map[string]int{}, maps.Collect(o.All()))

	o.Add("a", 12)
	assert.Equal(t, map[string]int{"a": 12}, maps.Collect(o.All()))

	o.Add("b", 22)
	o.Add("c", 33)

	assert.Equal(t, map[string]int{"a": 12, "b": 22, "c": 33}, maps.Collect(o.All()))
}
