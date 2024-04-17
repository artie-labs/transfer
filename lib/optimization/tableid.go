package optimization

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type TableIdentifier struct {
	database string
	schema   string
	name     string
}

func NewTableIdentifier(database, schema, name string) TableIdentifier {
	return TableIdentifier{database: database, schema: schema, name: name}
}

func (t TableIdentifier) Name(uppercaseEscNames bool, args *sql.NameArgs) string {
	return sql.EscapeName(t.name, uppercaseEscNames, args)
}

type FqNameOpts struct {
	BigQueryProjectID   string
	MsSQLSchemaOverride string
}

func (t TableIdentifier) FqName(kind constants.DestinationKind, escape bool, uppercaseEscNames bool, opts FqNameOpts) string {
	switch kind {
	case constants.S3:
		// S3 should be db.schema.tableName, but we don't need to escape, since it's not a SQL db.
		return fmt.Sprintf("%s.%s.%s", t.database, t.schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   false,
			DestKind: kind,
		}))
	case constants.Redshift:
		// Redshift is Postgres compatible, so when establishing a connection, we'll specify a database.
		// Thus, we only need to specify schema and table name here.
		return fmt.Sprintf("%s.%s", t.schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	case constants.MSSQL:
		return fmt.Sprintf("%s.%s", stringutil.Override(t.schema, opts.MsSQLSchemaOverride), t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	case constants.BigQuery:
		// The fully qualified name for BigQuery is: project_id.dataset.tableName.
		// We are escaping the project_id and dataset because there could be special characters.
		return fmt.Sprintf("`%s`.`%s`.%s", opts.BigQueryProjectID, t.database, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	default:
		return fmt.Sprintf("%s.%s.%s", t.database, t.schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	}
}
