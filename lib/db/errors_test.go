package db

import (
	"fmt"
	"syscall"
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
			name:           "direct error - connection refused",
			err:            syscall.ECONNREFUSED,
			expectedResult: true,
		},
		{
			name:           "direct error - connection reset",
			err:            syscall.ECONNRESET,
			expectedResult: true,
		},
		{
			name:           "wrapped error - connection refused",
			err:            fmt.Errorf("foo: %w", syscall.ECONNREFUSED),
			expectedResult: true,
		},
		{
			name:           "wrapped error - connection reset",
			err:            fmt.Errorf("foo: %w", syscall.ECONNRESET),
			expectedResult: true,
		},
	}

	for _, tc := range tcs {
		actualErr := retryableError(tc.err)
		assert.Equal(t, tc.expectedResult, actualErr, tc.name)
	}
}
