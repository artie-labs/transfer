package mssql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
