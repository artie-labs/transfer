package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = MySQLDialect{}

type TableIdentifier struct {
	database       string
	table          string
	temporaryTable bool
}

func NewTableIdentifier(database, table string) TableIdentifier {
	return TableIdentifier{database: database, table: table}
}

func (ti TableIdentifier) Database() string {
	return ti.database
}

// Schema returns the database name (MySQL uses database instead of schema)
func (ti TableIdentifier) Schema() string {
	return ti.database
}

func (ti TableIdentifier) EscapedTable() string {
	return _dialect.QuoteIdentifier(ti.table)
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.database, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s", _dialect.QuoteIdentifier(ti.database), ti.EscapedTable())
}

func (ti TableIdentifier) WithTemporaryTable(temp bool) sql.TableIdentifier {
	ti.temporaryTable = temp
	return ti
}

func (ti TableIdentifier) TemporaryTable() bool {
	return ti.temporaryTable
}
