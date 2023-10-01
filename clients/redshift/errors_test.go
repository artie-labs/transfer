package redshift

import (
	"fmt"

	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestRetryable_Error() {
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
		assert.Equal(r.T(), tc.expectedResult, actualErr, tc.name)
	}

}
