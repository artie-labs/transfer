package ddl

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestShouldDelete(t *testing.T) {
	type _testCase struct {
		name         string
		comment      string
		expectDelete bool
	}
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	oneHourFromNow := now.Add(1 * time.Hour)
	testCases := []_testCase{
		{
			name:         "random",
			comment:      "random",
			expectDelete: false,
		},
		{
			name:         "one hour from now, but no expires: prefix",
			comment:      typing.ExpiresDate(oneHourFromNow),
			expectDelete: false,
		},
		{
			name:         "one hour ago, but no expires: prefix",
			comment:      typing.ExpiresDate(oneHourAgo),
			expectDelete: false,
		},
		{
			name:         "one hour ago, with prefix, but extra space",
			comment:      fmt.Sprintf("%s %s", ExpireCommentPrefix, typing.ExpiresDate(oneHourAgo)),
			expectDelete: false,
		},
		{
			name:         "one hour from now, with prefix, but extra space",
			comment:      fmt.Sprintf("%s %s", ExpireCommentPrefix, typing.ExpiresDate(oneHourFromNow)),
			expectDelete: false,
		},
		{
			name:         "one hour ago (expired)",
			comment:      fmt.Sprintf("%s%s", ExpireCommentPrefix, typing.ExpiresDate(oneHourAgo)),
			expectDelete: true,
		},
		{
			name:         "one hour from now (not yet expired)",
			comment:      fmt.Sprintf("%s%s", ExpireCommentPrefix, typing.ExpiresDate(oneHourFromNow)),
			expectDelete: false,
		},
	}

	for _, testCase := range testCases {
		actualShouldDelete := ShouldDelete(testCase.comment)
		assert.Equal(t, testCase.expectDelete, actualShouldDelete, testCase.name)
	}
}

func TestShouldDeleteFromName(t *testing.T) {
	tblsToNotDelete := []string{
		"table", "table_", "table_abcdef9",
		fmt.Sprintf("future_table_%d", time.Now().Add(1*time.Hour).Unix()),
	}

	for _, tblToNotDelete := range tblsToNotDelete {
		assert.False(t, ShouldDeleteFromName(tblToNotDelete), tblToNotDelete)
	}

	tblsToDelete := []string{
		fmt.Sprintf("expired_table_%d", time.Now().Add(-1*time.Hour).Unix()),
		fmt.Sprintf("expired_tbl__artie_%d", time.Now().Add(-1*time.Hour).Unix()),
		fmt.Sprintf("expired_%d", time.Now().Add(-1*time.Hour).Unix()),
	}

	for _, tblToDelete := range tblsToDelete {
		assert.True(t, ShouldDeleteFromName(tblToDelete), tblToDelete)
	}
}
