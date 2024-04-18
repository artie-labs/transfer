package s3

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
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

func (ti TableIdentifier) Table() string {
	return ti.table
}

func (ti TableIdentifier) FullyQualifiedName(_, uppercaseEscNames bool) string {
	// S3 should be db.schema.tableName, but we don't need to escape, since it's not a SQL db.
	return fmt.Sprintf(
		"%s.%s.%s",
		ti.database,
		ti.schema,
		sql.EscapeName(ti.table, uppercaseEscNames, &sql.NameArgs{Escape: false, DestKind: constants.S3}),
	)
}
