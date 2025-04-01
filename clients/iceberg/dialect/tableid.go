package dialect

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = IcebergDialect{}

type TableIdentifier struct {
	catalog               string
	namespace             string
	table                 string
	disableDropProtection bool
}

func NewTableIdentifier(catalog, namespace, table string) TableIdentifier {
	return TableIdentifier{catalog: catalog, namespace: namespace, table: table}
}

func (ti TableIdentifier) Namespace() string {
	return ti.namespace
}

func (ti TableIdentifier) EscapedTable() string {
	return _dialect.QuoteIdentifier(ti.table)
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.catalog, ti.namespace, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s.%s", _dialect.QuoteIdentifier(ti.catalog), _dialect.QuoteIdentifier(ti.namespace), ti.EscapedTable())
}

func (ti TableIdentifier) WithDisableDropProtection(disableDropProtection bool) sql.TableIdentifier {
	ti.disableDropProtection = disableDropProtection
	return ti
}

func (ti TableIdentifier) AllowToDrop() bool {
	return ti.disableDropProtection
}
