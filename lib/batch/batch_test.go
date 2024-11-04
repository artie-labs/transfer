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

	testBySize := func(in []string, maxSizeBytes int, failIfRowExceedsMaxSizeBytes bool, encoder func(value string) ([]byte, error)) ([][][]byte, error) {
		batches := [][][]byte{}
		err := BySize(in, maxSizeBytes, failIfRowExceedsMaxSizeBytes, encoder, func(batch [][]byte) error { batches = append(batches, batch); return nil })
		return batches, err
	}

	badYield := func(batch [][]byte) error {
		out := make([]string, len(batch))
		for i, bytes := range batch {
			out[i] = string(bytes)
		}
		return fmt.Errorf("yield failed for %v", out)
	}

	{
		// Empty slice:
		batches, err := testBySize([]string{}, 10, true, panicEncoder)
		assert.NoError(t, err)
		assert.Empty(t, batches)
	}
	{
		// Non-empty slice + bad encoder:
		_, err := testBySize([]string{"foo", "bar"}, 10, true, badEncoder)
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
			_, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 20, true, goodEncoder)
			assert.ErrorContains(t, err, "item 1 is larger (23 bytes) than maxSizeBytes (20 bytes)")
		}
		{
			// failIfRowExceedsMaxSizeBytes = false
			batches, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar", "i-am-20-characters--"}, 20, false, goodEncoder)
			assert.NoError(t, err)
			assert.Len(t, batches, 2)
		}
	}
	{
		// Non-empty slice + item equal to maxSizeBytes:
		batches, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 23, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 3)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
		assert.Equal(t, [][]byte{[]byte("i-am-23-characters-long")}, batches[1])
		assert.Equal(t, [][]byte{[]byte("bar")}, batches[2])
	}
	{
		// Non-empty slice + one item:
		batches, err := testBySize([]string{"foo"}, 100, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
	}
	{
		// Non-empty slice + all items exactly fit into one batch:
		batches, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 12, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
	}
	{
		// Non-empty slice + all items exactly fit into just under one batch:
		batches, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 13, true, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
	}
}
