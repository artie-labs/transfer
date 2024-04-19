package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	// S3 doesn't escape the table name.
	tableID := NewTableIdentifier("database", "schema", "table")
	assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(true, false), "escaped")
	assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(true, true), "escaped + upper")
	assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(false, false), "unescaped")
	assert.Equal(t, "database.schema.table", tableID.FullyQualifiedName(false, true), "unescaped + upper")
}
