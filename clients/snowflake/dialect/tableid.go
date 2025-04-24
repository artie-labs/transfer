package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

var _dialect = SnowflakeDialect{}

type TableIdentifier struct {
	database              string
	schema                string
	table                 string
	randomFileName        string
	disableDropProtection bool
}

func NewTableIdentifier(database, schema, table string) TableIdentifier {
	ti := TableIdentifier{
		database: database,
		schema:   schema,
		table:    table,
	}

	ti.randomFileName = fmt.Sprintf("%s_%s.csv.gz", strings.ReplaceAll(ti.FullyQualifiedName(), `"`, ""), stringutil.Random(5))
	return ti
}

func (ti TableIdentifier) RandomFileName() (string, error) {
	if ti.randomFileName == "" {
		return "", fmt.Errorf("random file name is not set")
	}

	return ti.randomFileName, nil
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
	return fmt.Sprintf("%s.%s.%s", _dialect.QuoteIdentifier(ti.database), _dialect.QuoteIdentifier(ti.schema), ti.EscapedTable())
}

func (ti TableIdentifier) WithDisableDropProtection(disableDropProtection bool) sql.TableIdentifier {
	ti.disableDropProtection = disableDropProtection
	return ti
}

func (ti TableIdentifier) AllowToDrop() bool {
	return ti.disableDropProtection
}
