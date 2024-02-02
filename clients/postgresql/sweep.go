package postgresql

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (s *Store) Sweep() error {
	// TODO: Build an abstraction out for this, this is the same functionality as Redshift.

	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	// Find all the database and schema pairings
	// Then iterate over information schema
	// Find anything that has __artie__ in the table name
	// Find the comment
	// If the table should be killed, it will drop it.
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	dbAndSchemaPairs := kafkalib.GetUniqueDatabaseAndSchema(tcs)
	for _, dbAndSchemaPair := range dbAndSchemaPairs {
		// ILIKE is used to be case-insensitive since Snowflake stores all the tables in UPPER.
		var rows *sql.Rows
		rows, err = s.Store.Query(fmt.Sprintf(
			`select c.relname, d.description from pg_catalog.pg_description d
JOIN pg_class c on d.objoid = c.oid
JOIN pg_catalog.pg_namespace n on n.oid = c.relnamespace
WHERE n.nspname = '%s' and c.relname ILIKE '%s';`,
			dbAndSchemaPair.Schema,
			"%"+constants.ArtiePrefix+"%"))
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableName, comment string
			err = rows.Scan(&tableName, &comment)
			if err != nil {
				return err
			}

			if ddl.ShouldDelete(comment) {
				err = ddl.DropTemporaryTable(s,
					fmt.Sprintf("%s.%s.%s", dbAndSchemaPair.Database, dbAndSchemaPair.Schema, tableName), true)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
