package kafkalib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsRetryableErr(t *testing.T) {
	type _tc struct {
		err            error
		expectedResult bool
	}

	tcs := []_tc{
		{
			err:            nil,
			expectedResult: false,
		},
		{
			err:            fmt.Errorf("[30] Group Authorization Failed: the client is not authorized to access a particular group id"),
			expectedResult: true,
		},
	}

	for _, tc := range tcs {
		assert.Equal(t, tc.expectedResult, IsRetryableErr(tc.err), tc.err)
	}
}
