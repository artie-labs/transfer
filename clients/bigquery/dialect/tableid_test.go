package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("project", "dataset", "foo")
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "project", typedTableID2.ProjectID())
	assert.Equal(t, "dataset", typedTableID2.Dataset())
	assert.Equal(t, "bar", tableID2.Table())
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	// Table name that is not a reserved word:
	assert.Equal(t, "`project`.`dataset`.`foo`", NewTableIdentifier("project", "dataset", "foo").FullyQualifiedName())

	// Table name that is a reserved word:
	assert.Equal(t, "`project`.`dataset`.`table`", NewTableIdentifier("project", "dataset", "table").FullyQualifiedName())
}

func TestTableIdentifier_EscapedTable(t *testing.T) {
	// Table name that is not a reserved word:
	assert.Equal(t, "`foo`", NewTableIdentifier("project", "dataset", "foo").EscapedTable())

	// Table name that is a reserved word:
	assert.Equal(t, "`table`", NewTableIdentifier("project", "dataset", "table").EscapedTable())
}
