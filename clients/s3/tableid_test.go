package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("database", "schema", "foo", "")
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "database", typedTableID2.Database())
	assert.Equal(t, "schema", typedTableID2.Schema())
	assert.Equal(t, "bar", tableID2.Table())
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	{
		// S3 doesn't escape the table name.
		tableID := NewTableIdentifier("database", "schema", "table", "")
		assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName())
	}
	{
		// Separator via `/`
		tableID := NewTableIdentifier("database", "schema", "table", "/")
		assert.Equal(t, "database/schema/table", tableID.FullyQualifiedName())
	}
}

func TestTableIdentifier_EscapedTable(t *testing.T) {
	// S3 doesn't escape the table name.
	tableID := NewTableIdentifier("database", "schema", "table", "")
	assert.Equal(t, "table", tableID.EscapedTable())
}
