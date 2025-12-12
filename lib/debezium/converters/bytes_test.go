package converters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytes_Convert(t *testing.T) {
	{
		// nil
		actual, err := Bytes{}.Convert(nil)
		assert.NoError(t, err)
		assert.Nil(t, actual)
	}
	{
		// empty string - should return nil, not error
		actual, err := Bytes{}.Convert("")
		assert.NoError(t, err)
		assert.Nil(t, actual)
	}
	{
		// []byte
		actual, err := Bytes{}.Convert([]byte{40, 39, 38})
		assert.NoError(t, err)
		assert.Equal(t, []byte{40, 39, 38}, actual)
	}
	{
		// base64 encoded string
		actual, err := Bytes{}.Convert("aGVsbG8gd29ybGQK")
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0xa}, actual)
	}
	{
		// malformed string
		_, err := Bytes{}.Convert("asdf$$$")
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// type that is not string or []byte
		_, err := Bytes{}.Convert(map[string]any{})
		assert.ErrorContains(t, err, "expected []byte or string, got map[string]interface {}")
	}
}
