package redshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("schema", "foo", true)
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "schema", typedTableID2.Schema())
	assert.Equal(t, "bar", tableID2.Table())
	assert.True(t, typedTableID2.uppercaseEscapedNames)
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	// Table name that does not need escaping:
	assert.Equal(t, "schema.foo", NewTableIdentifier("schema", "foo", false).FullyQualifiedName())
	assert.Equal(t, "schema.foo", NewTableIdentifier("schema", "foo", true).FullyQualifiedName())

	// Table name that needs escaping:
	assert.Equal(t, `schema."table"`, NewTableIdentifier("schema", "table", false).FullyQualifiedName())
	assert.Equal(t, `schema."TABLE"`, NewTableIdentifier("schema", "table", true).FullyQualifiedName())
}
