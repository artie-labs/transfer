package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	schema                string
	table                 string
	uppercaseEscapedNames bool
}

func NewTableIdentifier(schema, table string, uppercaseEscapedNames bool) TableIdentifier {
	return TableIdentifier{schema: schema, table: table, uppercaseEscapedNames: uppercaseEscapedNames}
}

func (ti TableIdentifier) Schema() string {
	return ti.schema
}

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) WithTable(table string) types.TableIdentifier {
	return NewTableIdentifier(ti.schema, table, ti.uppercaseEscapedNames)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	// Redshift is Postgres compatible, so when establishing a connection, we'll specify a database.
	// Thus, we only need to specify schema and table name here.
	return fmt.Sprintf(
		"%s.%s",
		ti.schema,
		sql.EscapeNameIfNecessary(ti.table, ti.uppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: constants.Redshift}),
	)
}
