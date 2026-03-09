package stringutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGZipRoundTrip(t *testing.T) {
	{
		// Empty input
		compressed, err := GZipCompress([]byte{})
		assert.NoError(t, err)

		decompressed, err := GZipDecompress(compressed)
		assert.NoError(t, err)
		assert.Empty(t, decompressed)
	}
	{
		// Simple string
		data := []byte("hello, world!")
		compressed, err := GZipCompress(data)
		assert.NoError(t, err)
		assert.NotEqual(t, data, compressed)

		decompressed, err := GZipDecompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, data, decompressed)
	}
	{
		// Larger payload compresses smaller
		data := []byte("abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij" +
			"abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij")

		compressed, err := GZipCompress(data)
		assert.NoError(t, err)
		assert.Less(t, len(compressed), len(data))

		decompressed, err := GZipDecompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, data, decompressed)
	}
	{
		// Null bytes in the payload
		data := []byte{0x00, 0x00, 0x01, 0x00, 0xFF}
		compressed, err := GZipCompress(data)
		assert.NoError(t, err)

		decompressed, err := GZipDecompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, data, decompressed)
	}
}

func TestGZipDecompress_InvalidData(t *testing.T) {
	_, err := GZipDecompress([]byte("not gzip"))
	assert.ErrorContains(t, err, "unexpected EOF")
}
