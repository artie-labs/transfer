package dialect

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
	// Table name that is not a reserved word:
	assert.Equal(t, `schema."foo"`, NewTableIdentifier("schema", "foo").FullyQualifiedName())

	// Table name that is a reserved word:
	assert.Equal(t, `schema."table"`, NewTableIdentifier("schema", "table").FullyQualifiedName())
}

func TestTableIdentifier_EscapedTable(t *testing.T) {
	// Table name that is not a reserved word:
	assert.Equal(t, `"foo"`, NewTableIdentifier("schema", "foo").EscapedTable())

	// Table name that is a reserved word:
	assert.Equal(t, `"table"`, NewTableIdentifier("schema", "table").EscapedTable())
}
