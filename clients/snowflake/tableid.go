package snowflake

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
)

type TableIdentifier struct {
	database              string
	schema                string
	table                 string
	uppercaseEscapedNames bool
}

func NewTableIdentifier(database, schema, table string, uppercaseEscapedNames bool) TableIdentifier {
	return TableIdentifier{
		database:              database,
		schema:                schema,
		table:                 table,
		uppercaseEscapedNames: uppercaseEscapedNames,
	}
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

func (ti TableIdentifier) WithTable(table string) types.TableIdentifier {
	return NewTableIdentifier(ti.database, ti.schema, table, ti.uppercaseEscapedNames)
}

func (ti TableIdentifier) FullyQualifiedName() string {
	if sql.NeedsEscaping(ti.table, constants.Snowflake) && !ti.uppercaseEscapedNames {
		slog.Warn("Escaped Snowflake table is not being uppercased",
			slog.String("database", ti.database),
			slog.String("schema", ti.schema),
			slog.String("table", ti.table),
			slog.Bool("uppercaseEscapedNames", ti.uppercaseEscapedNames),
		)
	}

	return fmt.Sprintf(
		"%s.%s.%s",
		ti.database,
		ti.schema,
		sql.EscapeNameIfNecessary(ti.table, ti.uppercaseEscapedNames, constants.Snowflake),
	)
}
