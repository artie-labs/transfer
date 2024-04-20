package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("database", "schema", "foo")
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "database", typedTableID2.Database())
	assert.Equal(t, "schema", typedTableID2.Schema())
	assert.Equal(t, "bar", tableID2.Table())
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	{
		// Table name that does not need escaping:
		tableID := NewTableIdentifier("database", "schema", "foo")
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(false), "escaped")
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(true), "escaped + upper")
	}
	{
		// Table name that needs escaping:
		tableID := NewTableIdentifier("database", "schema", "table")
		assert.Equal(t, `database.schema."table"`, tableID.FullyQualifiedName(false), "escaped")
		assert.Equal(t, `database.schema."TABLE"`, tableID.FullyQualifiedName(true), "escaped + upper")
	}
}
