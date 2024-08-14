package ddl

import (
	"fmt"
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
			fmt.Sprintf("expired_tbl__artie_%d", time.Now().Add(constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("expired_tbl__notartie_%d", time.Now().Add(-1*time.Hour).Unix()),
		}

		for _, tblToNotDelete := range tablesToNotDrop {
			assert.False(t, ShouldDeleteFromName(tblToNotDelete), tblToNotDelete)
		}
	}
	{
		// Tables that are eligible to be dropped
		tablesToDrop := []string{
			fmt.Sprintf("tableName_%s_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("artie_%s_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
		}

		for _, tblToDelete := range tablesToDrop {
			assert.True(t, ShouldDeleteFromName(tblToDelete), tblToDelete)
		}
	}
}
