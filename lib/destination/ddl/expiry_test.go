package ddl

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
