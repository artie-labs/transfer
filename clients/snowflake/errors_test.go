package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthenticationExpirationErr(t *testing.T) {
	type _tc struct {
		err      error
		expected bool
	}

	tcs := []_tc{
		{
			err:      fmt.Errorf("390114: Authentication token has expired.  The user must authenticate again."),
			expected: true,
		},
		{
			err:      nil,
			expected: false,
		},
		{
			err:      fmt.Errorf("some random error"),
			expected: false,
		},
	}

	for idx, tc := range tcs {
		assert.Equal(t, tc.expected, IsAuthExpiredError(tc.err), idx)
	}
}
