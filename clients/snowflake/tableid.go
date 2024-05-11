package snowflake

import (
	"fmt"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/sql"
)

var _dialect = dialect.SnowflakeDialect{}

type TableIdentifier struct {
	database string
	schema   string
	table    string
}

func NewTableIdentifier(database, schema, table string) TableIdentifier {
	return TableIdentifier{
		database: database,
		schema:   schema,
		table:    table,
	}
}

func (ti TableIdentifier) Database() string {
	return ti.database
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
	return NewTableIdentifier(ti.database, ti.schema, table)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	return fmt.Sprintf("%s.%s.%s", ti.database, ti.schema, ti.EscapedTable())
}
