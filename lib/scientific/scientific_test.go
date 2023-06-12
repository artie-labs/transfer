package scientific

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsScientificNumber(t *testing.T) {
	type _testCase struct {
		name         string
		value        interface{}
		isScientific bool
		expectError  bool
	}

	testCases := []_testCase{
		{
			name:        "random number",
			value:       123,
			expectError: true,
		},
		{
			name:         "actual scientific number #1",
			value:        float64(58569107296622255421594597096899477504),
			isScientific: true,
		},
		{
			name:         "actual scientific number #2",
			value:        float64(58569102859845154622791691858438258688),
			isScientific: true,
		},
		{
			name:        "boolean",
			value:       true,
			expectError: true,
		},
		{
			name:        "array",
			value:       []int{1, 2, 3, 4},
			expectError: true,
		},
		{
			name:        "string",
			value:       "foo",
			expectError: true,
		},
		{
			name:        "struct",
			value:       `{"hello": "world"}`,
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.isScientific, IsScientificNumber(testCase.value), testCase.name)
		sha, err := ToSha256(testCase.value)
		if testCase.expectError {
			assert.Error(t, err, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
			assert.NotEmpty(t, sha, testCase.name)
		}
	}
}
