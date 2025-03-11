package batch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type key struct {
	foo string
}

func (k key) Key() string {
	return k.foo
}

func TestHasKeyFunction(t *testing.T) {
	{
		// False
		type noKey struct{}
		var _noKey noKey
		_, isOk := hasKeyFunction[noKey](_noKey)
		assert.False(t, isOk)
	}
	{
		// True
		_key := key{foo: "bar"}
		castedKey, isOk := hasKeyFunction[key](_key)
		assert.True(t, isOk)
		assert.Equal(t, "bar", castedKey.Key())
	}
}

func TestBySize(t *testing.T) {
	goodEncoder := func(value string) ([]byte, error) {
		return []byte(value), nil
	}

	panicEncoder := func(value string) ([]byte, error) {
		panic("should not be called")
	}

	badEncoder := func(value string) ([]byte, error) {
		return nil, fmt.Errorf("failed to encode %q", value)
	}

	testBySize := func(in []string, maxSizeBytes int, failIfRowExceedsMaxSizeBytes bool, encoder func(value string) ([]byte, error)) ([][][]byte, [][]string, error) {
		batches := [][][]byte{}
		items := [][]string{}
		err := BySize(in, maxSizeBytes, failIfRowExceedsMaxSizeBytes, encoder, func(batch [][]byte, batchItems []string) error {
			batches = append(batches, batch)
			items = append(items, batchItems)
			return nil
		})
		return batches, items, err
	}

	badYield := func(batch [][]byte, items []string) error {
		out := make([]string, len(batch))
		for i, bytes := range batch {
			out[i] = string(bytes)
		}
		return fmt.Errorf("yield failed for %v", out)
	}

	{
		// Empty slice:
		batches, items, err := testBySize([]string{}, 10, true, panicEncoder)
		assert.NoError(t, err)
		assert.Empty(t, batches)
		assert.Empty(t, items)
	}
	{
		// Non-empty slice + bad encoder:
		_, _, err := testBySize([]string{"foo", "bar"}, 10, true, badEncoder)
		assert.ErrorContains(t, err, `failed to encode item 0: failed to encode "foo"`)
	}
	{
		// Non-empty slice + two items that are < maxSizeBytes + yield returns error.
		err := BySize([]string{"foo", "bar"}, 10, true, goodEncoder, badYield)
		assert.ErrorContains(t, err, "yield failed for [foo bar]")
	}
	{
		// Non-empty slice + two items that are = maxSizeBytes + yield returns error.
		err := BySize([]string{"foo", "bar"}, 6, true, goodEncoder, badYield)
		assert.ErrorContains(t, err, "yield failed for [foo bar]")
	}
	{
		// Non-empty slice + two items that are > maxSizeBytes + yield returns error.
		err := BySize([]string{"foo", "bar-baz"}, 8, true, goodEncoder, badYield)
		assert.ErrorContains(t, err, "yield failed for [foo]")
	}
	{
		// Non-empty slice + item is larger than maxSizeBytes
		{
			// failIfRowExceedsMaxSizeBytes = true
			_, _, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 20, true, goodEncoder)
			assert.ErrorContains(t, err, "item 1 is larger (23 bytes) than maxSizeBytes (20 bytes)")
		}
		{
			// failIfRowExceedsMaxSizeBytes = false
			batches, items, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar", "i-am-20-characters--"}, 20, false, goodEncoder)
			assert.NoError(t, err)
			assert.Len(t, batches, 2)
			assert.Len(t, items, 2)
			// First batch should have foo and bar
			assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar")}, batches[0])
			assert.Equal(t, []string{"foo", "bar"}, items[0])
			// Second batch should have i-am-20-characters--
			assert.Equal(t, [][]byte{[]byte("i-am-20-characters--")}, batches[1])
			assert.Equal(t, []string{"i-am-20-characters--"}, items[1])
		}
	}
	{
		// Non-empty slice + item equal to maxSizeBytes:
		batches, items, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 23, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 3)
		assert.Len(t, items, 3)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
		assert.Equal(t, []string{"foo"}, items[0])
		assert.Equal(t, [][]byte{[]byte("i-am-23-characters-long")}, batches[1])
		assert.Equal(t, []string{"i-am-23-characters-long"}, items[1])
		assert.Equal(t, [][]byte{[]byte("bar")}, batches[2])
		assert.Equal(t, []string{"bar"}, items[2])
	}
	{
		// Non-empty slice + one item:
		batches, items, err := testBySize([]string{"foo"}, 100, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Len(t, items, 1)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
		assert.Equal(t, []string{"foo"}, items[0])
	}
	{
		// Non-empty slice + all items exactly fit into one batch:
		batches, items, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 12, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Len(t, items, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
		assert.Equal(t, []string{"foo", "bar", "baz", "qux"}, items[0])
	}
	{
		// Non-empty slice + all items exactly fit into just under one batch:
		batches, items, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 13, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Len(t, items, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
		assert.Equal(t, []string{"foo", "bar", "baz", "qux"}, items[0])
	}
}
