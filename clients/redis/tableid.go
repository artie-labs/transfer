package redis

import (
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
	return ti.table
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.namespace, ti.schema, table)
}

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

func (ti TableIdentifier) StreamKey() string {
	return ti.FullyQualifiedName()
}
