package s3

import (
	"cmp"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	database              string
	schema                string
	table                 string
	nameSeparator         string
	disableDropProtection bool
}

func NewTableIdentifier(database, schema, table string, nameSeparator string) TableIdentifier {
	return TableIdentifier{database: database, schema: schema, table: table, nameSeparator: cmp.Or(nameSeparator, ".")}
}

func (ti TableIdentifier) Database() string {
	return ti.database
}

func (ti TableIdentifier) Schema() string {
	return ti.schema
}

func (ti TableIdentifier) EscapedTable() string {
	// S3 doesn't require escaping
	return ti.table
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) sql.TableIdentifier {
	return NewTableIdentifier(ti.database, ti.schema, table, ti.nameSeparator)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	return strings.Join([]string{ti.database, ti.schema, ti.EscapedTable()}, ti.nameSeparator)
}

func (ti TableIdentifier) WithDisableDropProtection(disableDropProtection bool) sql.TableIdentifier {
	ti.disableDropProtection = disableDropProtection
	return ti
}

func (ti TableIdentifier) AllowToDrop() bool {
	return ti.disableDropProtection
}
