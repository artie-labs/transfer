package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigQuerySettings_Valid(t *testing.T) {
	type _testCase struct {
		name             string
		bigQuerySettings *BigQuerySettings
		expectError      bool
	}

	testCases := []_testCase{
		{
			name:        "nil",
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		actualErr := testCase.bigQuerySettings.Valid()
		if testCase.expectError {
			assert.Error(t, actualErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}
