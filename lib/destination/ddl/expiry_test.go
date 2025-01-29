package ddl

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestShouldDeleteFromName(t *testing.T) {
	{
		// Tables to not drop
		tablesToNotDrop := []string{
			"foo",
			"transactions",
			fmt.Sprintf("future_tbl___artie_suffix_%d", time.Now().Add(constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("future_tbl___notartie_%d", time.Now().Add(-1*time.Hour).Unix()),
			fmt.Sprintf("%s_foo_msm", constants.ArtiePrefix),
		}

		for _, tblToNotDelete := range tablesToNotDrop {
			assert.False(t, ShouldDeleteFromName(strings.ToLower(tblToNotDelete)), tblToNotDelete)
			assert.False(t, ShouldDeleteFromName(strings.ToUpper(tblToNotDelete)), tblToNotDelete)
			assert.False(t, ShouldDeleteFromName(tblToNotDelete), tblToNotDelete)
		}
	}
	{
		// Tables that are eligible to be dropped
		tablesToDrop := []string{
			"transactions___ARTIE_48GJC_1723663043",
			fmt.Sprintf("expired_tbl_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("artie_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
		}

		for _, tblToDelete := range tablesToDrop {
			assert.True(t, ShouldDeleteFromName(strings.ToLower(tblToDelete)), tblToDelete)
			assert.True(t, ShouldDeleteFromName(strings.ToUpper(tblToDelete)), tblToDelete)
			assert.True(t, ShouldDeleteFromName(tblToDelete), tblToDelete)
		}
	}
}
