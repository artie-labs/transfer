package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("database", "schema", "foo")
	tableID2 := tableID.WithTable("bar")
	assert.IsType(t, TableIdentifier{}, tableID2)
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
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "database.schema.foo", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
	{
		// Table name that needs escaping:
		tableID := NewTableIdentifier("database", "schema", "table")
		assert.Equal(t, `database.schema."table"`, tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, `database.schema."TABLE"`, tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
}
