package databricks

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestNewVolume(t *testing.T) {
	{
		// Invalid
		{
			// Missing name
			_, err := NewVolume(map[string]any{"path": "path"})
			assert.ErrorContains(t, err, "volume name is missing")
		}
		{
			// Name isn't string
			_, err := NewVolume(map[string]any{"name": 1, "path": "path"})
			assert.ErrorContains(t, err, "volume name is not a string")
		}
		{
			// Missing path
			_, err := NewVolume(map[string]any{"name": "name"})
			assert.ErrorContains(t, err, "volume path is missing")
		}
		{
			// Path isn't string
			_, err := NewVolume(map[string]any{"name": "name", "path": 1})
			assert.ErrorContains(t, err, "volume path is not a string")
		}
	}
	{
		// Valid
		volume, err := NewVolume(map[string]any{"name": "name", "path": "path"})
		assert.Nil(t, err)
		assert.Equal(t, "name", volume.name)
		assert.Equal(t, "path", volume.path)
	}
}

func newVolume(volName string) Volume {
	return Volume{
		name: volName,
	}
}

func TestVolume_ShouldDelete(t *testing.T) {
	{
		// Should delete
		volume := Volume{name: "name.csv"}
		assert.False(t, volume.ShouldDelete())
	}
	{
		// Tables that are eligible to be dropped
		tablesToDrop := []string{
			"transactions___ARTIE_48GJC_1723663043",
			fmt.Sprintf("expired_tbl_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("artie_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
		}

		for _, tblToDelete := range tablesToDrop {
			assert.True(t, newVolume(strings.ToLower(tblToDelete)).ShouldDelete(), tblToDelete)
			assert.True(t, newVolume(strings.ToUpper(tblToDelete)).ShouldDelete(), tblToDelete)
			assert.True(t, newVolume(tblToDelete).ShouldDelete(), tblToDelete)
		}
	}
}
