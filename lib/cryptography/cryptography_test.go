package cryptography

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashValue(t *testing.T) {
	{
		// If we pass nil in, we should get nil out (regardless of salt).
		assert.Equal(t, nil, HashValue(nil, ""))
		assert.Equal(t, nil, HashValue(nil, "pepper"))
	}
	{
		// Pass in an empty string
		assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", HashValue("", ""))
	}
	{
		// Pass in a string
		assert.Equal(t, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", HashValue("hello world", ""))
	}
	{
		// Value should be deterministic.
		for range 50 {
			assert.Equal(t, "b9a40320d82075681b2500e38160538e5e912bd8f49c03e87367fe82c1fa35d2", HashValue("dusty the mini aussie", ""))
		}
	}
	{
		// With a non-empty salt we should use HMAC-SHA256. The digest must differ from the unsalted one
		// and match the known HMAC-SHA256 output.
		salted := HashValue("hello world", "pepper")
		assert.NotEqual(t, HashValue("hello world", ""), salted)
		assert.Equal(t, "7e0d88baf0b87469157ec93e56df77d661d378f67e2912f5e9b40d8f541e7243", salted)

		// Deterministic across calls.
		for range 50 {
			assert.Equal(t, "7e0d88baf0b87469157ec93e56df77d661d378f67e2912f5e9b40d8f541e7243", HashValue("hello world", "pepper"))
		}
	}
}

func BenchmarkHashValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		assert.Equal(b, "b9a40320d82075681b2500e38160538e5e912bd8f49c03e87367fe82c1fa35d2", HashValue("dusty the mini aussie", ""))
	}
}
