package bigquery

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
	{
		// Table name that does not need escaping:
		tableID := NewTableIdentifier("project", "dataset", "foo")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(false), "escaped")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(true), "escaped + upper")
	}
	{
		// Table name that needs escaping:
		tableID := NewTableIdentifier("project", "dataset", "table")
		assert.Equal(t, "`project`.`dataset`.`table`", tableID.FullyQualifiedName(false), "escaped")
		assert.Equal(t, "`project`.`dataset`.`TABLE`", tableID.FullyQualifiedName(true), "escaped + upper")
	}
}
