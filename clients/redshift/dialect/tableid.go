package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = RedshiftDialect{}

type TableIdentifier struct {
	schema         string
	table          string
	temporaryTable bool
}

func NewTableIdentifier(schema, table string) TableIdentifier {
	return TableIdentifier{schema: schema, table: table}
}

func (ti TableIdentifier) Schema() string {
	return ti.schema
}

func (ti TableIdentifier) EscapedTable() string {
	return _dialect.QuoteIdentifier(ti.table)
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.schema, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	// Redshift is Postgres compatible, so when establishing a connection, we'll specify a database.
	// Thus, we only need to specify schema and table name here.
	return fmt.Sprintf("%s.%s", ti.schema, ti.EscapedTable())
}

func (ti TableIdentifier) WithTemporaryTable(temp bool) sql.TableIdentifier {
	ti.temporaryTable = temp
	return ti
}

func (ti TableIdentifier) TemporaryTable() bool {
	return ti.temporaryTable
}
