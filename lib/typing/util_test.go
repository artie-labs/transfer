package typing

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertType(t *testing.T) {
	{
		// String to string
		val, err := AssertType[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Int to string
		_, err := AssertType[string](1)
		assert.ErrorContains(t, err, "expected type string, got int")
	}
	{
		// Boolean to boolean
		val, err := AssertType[bool](true)
		assert.NoError(t, err)
		assert.Equal(t, true, val)
	}
	{
		// String to boolean
		_, err := AssertType[bool]("true")
		assert.ErrorContains(t, err, "expected type bool, got string")
	}
}

func TestAssertTypeOptional(t *testing.T) {
	{
		// String to string
		val, err := AssertTypeOptional[string]("hello")
		assert.NoError(t, err)
		assert.Equal(t, "hello", val)
	}
	{
		// Nil to string
		val, err := AssertTypeOptional[string](nil)
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}
}

func TestDefaultValueFromPtr(t *testing.T) {
	{
		// ptr is not set
		assert.Equal(t, int32(5), DefaultValueFromPtr[int32](nil, int32(5)))
	}
	{
		// ptr is set
		assert.Equal(t, int32(10), DefaultValueFromPtr[int32](ToPtr(int32(10)), int32(5)))
	}
}

func Test_IsJSON(t *testing.T) {
	{
		invalidValues := []string{
			`{"hello": "world"`,
			`{"hello": "world"}}`,
			`{null}`,
			"",
			"foo",
			"  ",
			"{",
			"[",
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
