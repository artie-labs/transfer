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
