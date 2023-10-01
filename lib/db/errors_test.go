package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryable_Errors(t *testing.T) {
	type _tc struct {
		name           string
		err            error
		expectedResult bool
	}

	tcs := []_tc{
		{
			name:           "nil error",
			err:            nil,
			expectedResult: false,
		},
		{
			name:           "irrelevant error",
			err:            fmt.Errorf("random error"),
			expectedResult: false,
		},
		{
			name:           "retryable error",
			err:            fmt.Errorf("error: read tcp 127.0.0.1:40104->127.0.0.1:28889: read: connection reset by peer"),
			expectedResult: true,
		},
	}

	for _, tc := range tcs {
		actualErr := retryableError(tc.err)
		assert.Equal(t, tc.expectedResult, actualErr, tc.name)
	}
}
