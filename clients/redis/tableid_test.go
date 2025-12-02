package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier(t *testing.T) {
	ti := NewTableIdentifier("mydb", "public", "users")

	assert.Equal(t, "mydb", ti.Database())
	assert.Equal(t, "public", ti.Schema())
	assert.Equal(t, "users", ti.Table())
	assert.Equal(t, "users", ti.EscapedTable())
	assert.Equal(t, "mydb:public:users", ti.FullyQualifiedName())
	assert.Equal(t, "mydb:public:users", ti.StreamKey())
}

func TestTableIdentifier_NoNamespace(t *testing.T) {
	ti := NewTableIdentifier("", "public", "users")

	assert.Equal(t, "", ti.Database())
	assert.Equal(t, "public", ti.Schema())
	assert.Equal(t, "users", ti.Table())
	assert.Equal(t, "public:users", ti.FullyQualifiedName())
	assert.Equal(t, "public:users", ti.StreamKey())
}

func TestTableIdentifier_NoSchema(t *testing.T) {
	ti := NewTableIdentifier("mydb", "", "users")

	assert.Equal(t, "mydb", ti.Database())
	assert.Equal(t, "", ti.Schema())
	assert.Equal(t, "users", ti.Table())
	assert.Equal(t, "mydb:users", ti.FullyQualifiedName())
	assert.Equal(t, "mydb:users", ti.StreamKey())
}

func TestTableIdentifier_WithTable(t *testing.T) {
	ti := NewTableIdentifier("mydb", "public", "users")
	newTI := ti.WithTable("orders").(TableIdentifier)

	assert.Equal(t, "mydb", newTI.Database())
	assert.Equal(t, "public", newTI.Schema())
	assert.Equal(t, "orders", newTI.Table())
	assert.Equal(t, "mydb:public:orders", newTI.FullyQualifiedName())
}

func TestTableIdentifier_TemporaryTable(t *testing.T) {
	ti := NewTableIdentifier("mydb", "public", "users")

	assert.False(t, ti.TemporaryTable())

	// Redis doesn't support temporary tables, so this should return the same
	tiTemp := ti.WithTemporaryTable(true)
	assert.False(t, tiTemp.TemporaryTable())
}
