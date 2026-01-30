package sqs

import (
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	database string
	schema   string
	table    string
}

func NewTableIdentifier(database, schema, table string) TableIdentifier {
	return TableIdentifier{database: database, schema: schema, table: table}
}

func (ti TableIdentifier) Database() string {
	return ti.database
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
	return NewTableIdentifier(ti.database, ti.schema, table)
}

// FullyQualifiedName returns the queue name for per-table mode: database_schema_table
func (ti TableIdentifier) FullyQualifiedName() string {
	parts := []string{}
	if ti.database != "" {
		parts = append(parts, ti.database)
	}
	if ti.schema != "" {
		parts = append(parts, ti.schema)
	}
	parts = append(parts, ti.table)
	return strings.Join(parts, "_")
}

func (ti TableIdentifier) WithTemporaryTable(_ bool) sql.TableIdentifier {
	return ti
}

func (ti TableIdentifier) TemporaryTable() bool {
	return false
}

// QueueName returns the SQS queue name for this table (used in per-table mode)
func (ti TableIdentifier) QueueName() string {
	return ti.FullyQualifiedName()
}
