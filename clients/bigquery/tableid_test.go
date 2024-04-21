package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_WithTable(t *testing.T) {
	tableID := NewTableIdentifier("project", "dataset", "foo", true)
	tableID2 := tableID.WithTable("bar")
	typedTableID2, ok := tableID2.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "project", typedTableID2.ProjectID())
	assert.Equal(t, "dataset", typedTableID2.Dataset())
	assert.Equal(t, "bar", tableID2.Table())
	assert.True(t, typedTableID2.uppercaseEscapedNames)
}

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	// Table name that does not need escaping:
	assert.Equal(t, "`project`.`dataset`.foo", NewTableIdentifier("project", "dataset", "foo", false).FullyQualifiedName())
	assert.Equal(t, "`project`.`dataset`.foo", NewTableIdentifier("project", "dataset", "foo", true).FullyQualifiedName())

	// Table name that needs escaping:
	assert.Equal(t, "`project`.`dataset`.`table`", NewTableIdentifier("project", "dataset", "table", false).FullyQualifiedName())
	assert.Equal(t, "`project`.`dataset`.`TABLE`", NewTableIdentifier("project", "dataset", "table", true).FullyQualifiedName())
}
