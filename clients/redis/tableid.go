package redis

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	namespace string
	schema    string
	table     string
}

func NewTableIdentifier(namespace, schema, table string) TableIdentifier {
	return TableIdentifier{namespace: namespace, schema: schema, table: table}
}

func (ti TableIdentifier) Database() string {
	return ti.namespace
}

func (ti TableIdentifier) Schema() string {
	return ti.schema
}

func (ti TableIdentifier) EscapedTable() string {
	// Redis doesn't require escaping
	return ti.table
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.namespace, ti.schema, table)
}

// FullyQualifiedName returns the Redis key prefix: namespace:schema:table
func (ti TableIdentifier) FullyQualifiedName() string {
	parts := []string{}
	if ti.namespace != "" {
		parts = append(parts, ti.namespace)
	}
	if ti.schema != "" {
		parts = append(parts, ti.schema)
	}
	parts = append(parts, ti.table)
	return strings.Join(parts, ":")
}

func (ti TableIdentifier) WithTemporaryTable(temp bool) sql.TableIdentifier {
	return ti
}

func (ti TableIdentifier) TemporaryTable() bool {
	return false
}

// KeyPattern returns the pattern for this table's keys: namespace:schema:table:*
func (ti TableIdentifier) KeyPattern() string {
	return fmt.Sprintf("%s:*", ti.FullyQualifiedName())
}

// CounterKey returns the key used for generating IDs: namespace:schema:table:__counter
func (ti TableIdentifier) CounterKey() string {
	return fmt.Sprintf("%s:__counter", ti.FullyQualifiedName())
}

// RecordKey returns the key for a specific record: namespace:schema:table:id
func (ti TableIdentifier) RecordKey(id int64) string {
	return fmt.Sprintf("%s:%d", ti.FullyQualifiedName(), id)
}
