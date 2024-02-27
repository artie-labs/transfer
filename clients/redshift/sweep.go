package redshift

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (s *Store) Sweep() error {
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
		rows, err = s.Store.Query(`
SELECT c.relname, COALESCE(d.description, '')
FROM pg_catalog.pg_class c
FULL OUTER JOIN pg_catalog.pg_description d on d.objoid = c.oid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname ILIKE $2;`, dbAndSchemaPair.Schema, "%"+constants.ArtiePrefix+"%")
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableName, comment string
			err = rows.Scan(&tableName, &comment)
			if err != nil {
				return err
			}
			// TODO: Deprecate the use of comments and standardize on ShouldDeleteFromName
			// Combine Sweep (Redshift, Snowflake, MSSQL)
			if ddl.ShouldDeleteFromName(tableName) || ddl.ShouldDelete(comment) {
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
