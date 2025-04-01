package databricks

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/lib/config/constants"
)

func TestNewFile(t *testing.T) {
	{
		// Invalid
		{
			// Missing name
			_, err := NewFile(map[string]any{"path": "path"})
			assert.ErrorContains(t, err, `key: "name" does not exist in object`)
		}
		{
			// Name isn't string
			_, err := NewFile(map[string]any{"name": 1, "path": "path"})
			assert.ErrorContains(t, err, `expected type string, got int`)
		}
		{
			// Missing path
			_, err := NewFile(map[string]any{"name": "name"})
			assert.ErrorContains(t, err, `key: "path" does not exist in object`)
		}
		{
			// Path isn't string
			_, err := NewFile(map[string]any{"name": "name", "path": 1})
			assert.ErrorContains(t, err, "expected type string, got int")
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

	parts := strings.Split(file.DBFSFilePath(), "/")
	assert.Len(t, parts, 6)

	assert.Equal(t, "dbfs:", parts[0])
	assert.Equal(t, "Volumes", parts[1])
	assert.Equal(t, "{DB}", parts[2])
	assert.Equal(t, "{SCHEMA}", parts[3])
	assert.Equal(t, "{VOLUME}", parts[4])

	// Last one has a random suffix
	assert.Contains(t, parts[5], "{TABLE}")
	assert.Contains(t, parts[5], ".csv.gz")
}
