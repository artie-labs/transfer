package redshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("schema", "foo")
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "schema", typedTableID2.Schema())
	assert.Equal(t, "bar", tableID2.Table())
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	{
		// Table name that does not need escaping:
		tableID := NewTableIdentifier("schema", "foo")
		assert.Equal(t, "schema.foo", tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, "schema.foo", tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "schema.foo", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "schema.foo", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
	{
		// Table name that needs escaping:
		tableID := NewTableIdentifier("schema", "table")
		assert.Equal(t, `schema."table"`, tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, `schema."TABLE"`, tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "schema.table", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "schema.table", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
}
