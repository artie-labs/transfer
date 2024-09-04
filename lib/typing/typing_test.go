package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsJSON(t *testing.T) {
	{
		invalidValues := []string{
			`{"hello": "world"`,
			`{"hello": "world"}}`,
			`{null}`,
			"",
			"  ",
		}

		for _, invalidValue := range invalidValues {
			assert.False(t, IsJSON(invalidValue), invalidValue)
		}
	}
	{
		validValues := []string{
			"{}",
			`{"hello": "world"}`,
			`{
				"hello": {
					"world": {
						"nested_value": true
					}
				},
				"add_a_list_here": [1, 2, 3, 4],
				"number": 7.5,
				"integerNum": 7
			}`,
			"[]",
			"[1, 2, 3, 4]",
		}

		for _, validValue := range validValues {
			assert.True(t, IsJSON(validValue), validValue)
		}
	}
}

func TestToBytes(t *testing.T) {
	{
		// []byte
		actual, err := ToBytes([]byte{40, 39, 38})
		assert.NoError(t, err)
		assert.Equal(t, []byte{40, 39, 38}, actual)
	}
	{
		// base64 encoded string
		actual, err := ToBytes("aGVsbG8gd29ybGQK")
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0xa}, actual)
	}
	{
		// malformed string
		_, err := ToBytes("asdf$$$")
		assert.ErrorContains(t, err, "failed to base64 decode")
	}
	{
		// type that is not string or []byte
		_, err := ToBytes(map[string]any{})
		assert.ErrorContains(t, err, "failed to cast value 'map[]' with type 'map[string]interface {}' to []byte")
	}
}
