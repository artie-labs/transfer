package typing

import (
	"encoding/json"
	"strings"
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
			"foo",
			"  ",
			"12345",
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

func BenchmarkIsJSON(b *testing.B) {
	values := []string{"hello world", `{"hello": "world"}`, `{"hello": "world"}}`, `{null}`, "", "foo", "  ", "12345"}
	b.Run("OldMethod", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, value := range values {
				oldIsJSON(value)
			}
		}
	})

	b.Run("NewMethod", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, value := range values {
				IsJSON(value)
			}
		}
	})
}

func oldIsJSON(str string) bool {
	str = strings.TrimSpace(str)
	if len(str) < 2 {
		return false
	}

	valStringChars := []rune(str)
	firstChar := string(valStringChars[0])
	lastChar := string(valStringChars[len(valStringChars)-1])

	if (firstChar == "{" && lastChar == "}") || (firstChar == "[" && lastChar == "]") {
		var js json.RawMessage
		return json.Unmarshal([]byte(str), &js) == nil
	}

	return false
}
