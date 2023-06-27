package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthenticationExpirationErr(t *testing.T) {
	assert.Equal(t, true, AuthenticationExpirationErr(fmt.Errorf("390114: Authentication token has expired.  The user must authenticate again.")))
}
