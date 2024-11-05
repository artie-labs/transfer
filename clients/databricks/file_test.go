package databricks

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/clients/databricks/dialect"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestNewFile(t *testing.T) {
	{
		// Invalid
		{
			// Missing name
			_, err := NewFile(map[string]any{"path": "path"})
			assert.ErrorContains(t, err, "name is missing")
		}
		{
			// Name isn't string
			_, err := NewFile(map[string]any{"name": 1, "path": "path"})
			assert.ErrorContains(t, err, "name is not a string")
		}
		{
			// Missing path
			_, err := NewFile(map[string]any{"name": "name"})
			assert.ErrorContains(t, err, "path is missing")
		}
		{
			// Path isn't string
			_, err := NewFile(map[string]any{"name": "name", "path": 1})
			assert.ErrorContains(t, err, "path is not a string")
		}
	}
	{
		// Valid
		file, err := NewFile(map[string]any{"name": "name", "path": "path"})
		assert.Nil(t, err)
		assert.Equal(t, "name", file.name)
		assert.Equal(t, "path", file.FilePath())
	}
}

func newFile(name string) File {
	return File{name: name}
}

func TestFile_ShouldDelete(t *testing.T) {
	{
		assert.False(t, File{name: "name.csv"}.ShouldDelete())
	}
	{
		tablesToDrop := []string{
			"transactions___ARTIE_48GJC_1723663043",
			fmt.Sprintf("expired_tbl_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
			fmt.Sprintf("artie_%s_suffix_%d", constants.ArtiePrefix, time.Now().Add(-1*constants.TemporaryTableTTL).Unix()),
		}

		for _, tblToDelete := range tablesToDrop {
			assert.True(t, newFile(strings.ToLower(tblToDelete)).ShouldDelete(), tblToDelete)
			assert.True(t, newFile(strings.ToUpper(tblToDelete)).ShouldDelete(), tblToDelete)
			assert.True(t, newFile(tblToDelete).ShouldDelete(), tblToDelete)
		}
	}
}

func TestFile_DBFSFilePath(t *testing.T) {
	file := NewFileFromTableID(dialect.NewTableIdentifier("{DB}", "{SCHEMA}", "{TABLE}"), "{VOLUME}")
	assert.Equal(t, "dbfs:/Volumes/{DB}/{SCHEMA}/{VOLUME}/{TABLE}.csv", file.DBFSFilePath())
}
