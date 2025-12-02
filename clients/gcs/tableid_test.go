package gcs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	{
		// Default separator
		ti := NewTableIdentifier("db", "schema", "table", "")
		assert.Equal(t, "db.schema.table", ti.FullyQualifiedName())
	}
	{
		// Custom separator
		ti := NewTableIdentifier("db", "schema", "table", "_")
		assert.Equal(t, "db_schema_table", ti.FullyQualifiedName())
	}
}

func TestTableIdentifier_WithTable(t *testing.T) {
	ti := NewTableIdentifier("db", "schema", "table", ".")
	newTi := ti.WithTable("new_table")
	assert.Equal(t, "db.schema.new_table", newTi.FullyQualifiedName())
}

func TestTableIdentifier_TemporaryTable(t *testing.T) {
	ti := NewTableIdentifier("db", "schema", "table", ".")
	assert.False(t, ti.TemporaryTable())

	tempTi := ti.WithTemporaryTable(true)
	assert.True(t, tempTi.TemporaryTable())
}
