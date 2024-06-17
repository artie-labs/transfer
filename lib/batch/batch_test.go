package batch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	testBySize := func(in []string, maxSize int, encoder func(value string) ([]byte, error)) ([][][]byte, error) {
		out := [][][]byte{}
		err := BySize(in, maxSize, encoder, func(batch [][]byte) error { out = append(out, batch); return nil })
		return out, err
	}

	{
		// Empty slice:
		batches, err := testBySize([]string{}, 0, panicEncoder)
		assert.NoError(t, err)
		assert.Empty(t, batches)
	}
	{
		// Non-empty slice + bad encoder:
		_, err := testBySize([]string{"foo", "bar"}, 10, badEncoder)
		assert.ErrorContains(t, err, `failed to encode item 0: failed to encode "foo"`)
	}
	{
		// Non-empty slice + two items that are < maxSize + yield returns error.
		err := BySize([]string{"foo", "bar"}, 10, goodEncoder, func(batch [][]byte) error { return fmt.Errorf("yield failed") })
		assert.ErrorContains(t, err, "yield failed")
	}
	{
		// Non-empty slice + two items that are = maxSize + yield returns error.
		err := BySize([]string{"foo", "bar"}, 6, goodEncoder, func(batch [][]byte) error { return fmt.Errorf("yield failed") })
		assert.ErrorContains(t, err, "yield failed")
	}
	{
		// Non-empty slice + two items that are > maxSize + yield returns error.
		err := BySize([]string{"foo", "bar-baz"}, 8, goodEncoder, func(batch [][]byte) error { return fmt.Errorf("yield failed") })
		assert.ErrorContains(t, err, "yield failed")
	}
	{
		// Non-empty slice + item is larger than max size:
		_, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 20, goodEncoder)
		assert.ErrorContains(t, err, "item 1 is larger (23 bytes) than max size (20 bytes)")
	}
	{
		// Non-empty slice + item equal to max size:
		batches, err := testBySize([]string{"foo", "i-am-23-characters-long", "bar"}, 23, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 3)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
		assert.Equal(t, [][]byte{[]byte("i-am-23-characters-long")}, batches[1])
		assert.Equal(t, [][]byte{[]byte("bar")}, batches[2])
	}
	{
		// Non-empty slice + one item:
		batches, err := testBySize([]string{"foo"}, 100, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo")}, batches[0])
	}
	{
		// Non-empty slice + all items exactly fit into one batch:
		batches, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 12, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
	}
	{
		// Non-empty slice + all items exactly fit into just under one batch:
		batches, err := testBySize([]string{"foo", "bar", "baz", "qux"}, 13, goodEncoder)
		assert.NoError(t, err)
		assert.Len(t, batches, 1)
		assert.Equal(t, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}, batches[0])
	}
}
