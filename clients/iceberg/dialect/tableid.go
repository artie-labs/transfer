package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = IcebergDialect{}

type TableIdentifier struct {
	catalog        string
	namespace      string
	table          string
	temporaryTable bool
}

func NewTableIdentifier(catalog, namespace, table string) TableIdentifier {
	return TableIdentifier{catalog: catalog, namespace: namespace, table: table}
}

func (ti TableIdentifier) Namespace() string {
	return strings.ToLower(ti.namespace)
}

func (ti TableIdentifier) Schema() string {
	return ti.Namespace()
}

func (ti TableIdentifier) EscapedTable() string {
	return _dialect.QuoteIdentifier(ti.table)
}

func (ti TableIdentifier) Table() string {
	return strings.ToLower(ti.table)
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.catalog, ti.namespace, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s.%s", _dialect.QuoteIdentifier(ti.catalog), _dialect.QuoteIdentifier(ti.namespace), ti.EscapedTable())
}

func (ti TableIdentifier) WithTemporaryTable(temp bool) sql.TableIdentifier {
	ti.temporaryTable = temp
	return ti
}

func (ti TableIdentifier) TemporaryTable() bool {
	return ti.temporaryTable
}
