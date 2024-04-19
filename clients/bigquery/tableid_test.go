package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	{
		// Table name that does not need escaping:
		tableID := NewTableIdentifier("project", "dataset", "foo")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "`project`.`dataset`.foo", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
	{
		// Table name that needs escaping:
		tableID := NewTableIdentifier("project", "dataset", "table")
		assert.Equal(t, "`project`.`dataset`.`table`", tableID.FullyQualifiedName(true, false), "escaped")
		assert.Equal(t, "`project`.`dataset`.`TABLE`", tableID.FullyQualifiedName(true, true), "escaped + upper")
		assert.Equal(t, "`project`.`dataset`.table", tableID.FullyQualifiedName(false, false), "unescaped")
		assert.Equal(t, "`project`.`dataset`.table", tableID.FullyQualifiedName(false, true), "unescaped + upper")
	}
}
