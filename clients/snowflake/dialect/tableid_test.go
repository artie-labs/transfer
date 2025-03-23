package dialect

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
	assert.Equal(t, `"DATABASE"."SCHEMA"."FOO"`, NewTableIdentifier("database", "schema", "foo").FullyQualifiedName())

	// Table name that is a reserved word:
	assert.Equal(t, `"DATABASE"."SCHEMA"."TABLE"`, NewTableIdentifier("database", "schema", "table").FullyQualifiedName())
}

func TestTableIdentifier_EscapedTable(t *testing.T) {
	// Table name that is not a reserved word:
	assert.Equal(t, `"FOO"`, NewTableIdentifier("database", "schema", "foo").EscapedTable())

	// Table name that is a reserved word:
	assert.Equal(t, `"TABLE"`, NewTableIdentifier("database", "schema", "table").EscapedTable())
}

func TestTableIdentifier_StagingFileName(t *testing.T) {
	assert.Equal(t, `database_schema_foo.csv`, NewTableIdentifier("database", "schema", "foo").StagingFileName())
	assert.Equal(t, `db_schema_foo.csv`, NewTableIdentifier("DB", "SCHEMA", "FOO").StagingFileName())
}
