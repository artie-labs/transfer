package sqs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIdentifier_FullyQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		database string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "all parts",
			database: "postgres",
			schema:   "public",
			table:    "users",
			expected: "postgres_public_users",
		},
		{
			name:     "no database",
			database: "",
			schema:   "public",
			table:    "users",
			expected: "public_users",
		},
		{
			name:     "no schema",
			database: "postgres",
			schema:   "",
			table:    "users",
			expected: "postgres_users",
		},
		{
			name:     "table only",
			database: "",
			schema:   "",
			table:    "users",
			expected: "users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ti := NewTableIdentifier(tc.database, tc.schema, tc.table)
			assert.Equal(t, tc.expected, ti.FullyQualifiedName())
			assert.Equal(t, tc.expected, ti.QueueName())
		})
	}
}

func TestTableIdentifier_Accessors(t *testing.T) {
	ti := NewTableIdentifier("mydb", "myschema", "mytable")

	assert.Equal(t, "mydb", ti.Database())
	assert.Equal(t, "myschema", ti.Schema())
	assert.Equal(t, "mytable", ti.Table())
	assert.Equal(t, "mytable", ti.EscapedTable())
	assert.False(t, ti.TemporaryTable())
}

func TestTableIdentifier_WithTable(t *testing.T) {
	ti := NewTableIdentifier("db", "schema", "table1")
	newTi := ti.WithTable("table2")

	sqsTi, ok := newTi.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "db", sqsTi.Database())
	assert.Equal(t, "schema", sqsTi.Schema())
	assert.Equal(t, "table2", sqsTi.Table())
}

func TestTableIdentifier_WithTemporaryTable(t *testing.T) {
	ti := NewTableIdentifier("db", "schema", "table")

	// WithTemporaryTable should return the same identifier (SQS doesn't support temp tables)
	tempTi := ti.WithTemporaryTable(true)
	assert.Equal(t, ti, tempTi)

	nonTempTi := ti.WithTemporaryTable(false)
	assert.Equal(t, ti, nonTempTi)
}
