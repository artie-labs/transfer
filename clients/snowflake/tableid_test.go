package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("database", "schema", "foo", true)
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "database", typedTableID2.Database())
	assert.Equal(t, "schema", typedTableID2.Schema())
	assert.Equal(t, "bar", tableID2.Table())
	assert.True(t, typedTableID2.uppercaseEscapedNames)
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	// Table name that does not need escaping:
	assert.Equal(t, "database.schema.foo", NewTableIdentifier("database", "schema", "foo", false).FullyQualifiedName())
	assert.Equal(t, "database.schema.foo", NewTableIdentifier("database", "schema", "foo", true).FullyQualifiedName())

	// Table name that needs escaping:
	assert.Equal(t, `database.schema."table"`, NewTableIdentifier("database", "schema", "table", false).FullyQualifiedName())
	assert.Equal(t, `database.schema."TABLE"`, NewTableIdentifier("database", "schema", "table", true).FullyQualifiedName())
}
