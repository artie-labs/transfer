package redshift

import (
	"context"
	"fmt"

	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestIsRetryableCopyError() {
	{
		// Context cancelled - not retryable
		assert.False(r.T(), isRetryableCopyError(context.Canceled))
	}
	{
		// Context deadline exceeded - not retryable
		assert.False(r.T(), isRetryableCopyError(context.DeadlineExceeded))
	}
	{
		// stl_load_errors - deterministic data error, not retryable
		assert.False(r.T(), isRetryableCopyError(fmt.Errorf("failed to run COPY for temporary table: ERROR: Load into table 'alloy_transactions_v2' failed.  Check 'stl_load_errors' system table for details. (SQLSTATE XX000)")))
	}
	{
		// Transient network error - retryable
		assert.True(r.T(), isRetryableCopyError(fmt.Errorf("connection refused")))
	}
	{
		// Generic Redshift error without stl_load_errors - retryable
		assert.True(r.T(), isRetryableCopyError(fmt.Errorf("ERROR: could not connect to server")))
	}
}
