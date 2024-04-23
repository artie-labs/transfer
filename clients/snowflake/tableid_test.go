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
	// Table name that is not a reserved word:
	assert.Equal(t, "database.schema.foo", NewTableIdentifier("database", "schema", "foo").FullyQualifiedName())

	// Table name that is a reserved word:
	assert.Equal(t, `database.schema."TABLE"`, NewTableIdentifier("database", "schema", "table").FullyQualifiedName())
}
