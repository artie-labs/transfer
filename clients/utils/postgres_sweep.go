package utils

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func PostgresSweep(store destination.DataWarehouse, tcs []*kafkalib.TopicConfig) error {
	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	// Find all the database and schema pairings
	// Then iterate over information schema
	// Find anything that has __artie__ in the table name
	// Find the comment
	// If the table should be killed, it will drop it.
	dbAndSchemaPairs := kafkalib.GetUniqueDatabaseAndSchema(tcs)
	for _, dbAndSchemaPair := range dbAndSchemaPairs {
		var rows *sql.Rows
		rows, err := store.Query(
			`select c.relname, d.description from pg_catalog.pg_description d
JOIN pg_class c on d.objoid = c.oid
JOIN pg_catalog.pg_namespace n on n.oid = c.relnamespace
WHERE n.nspname = $1 and c.relname ILIKE $2;`, dbAndSchemaPair.Schema, "%"+constants.ArtiePrefix+"%")
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
				fqTableName := fmt.Sprintf("%s.%s.%s", dbAndSchemaPair.Database, dbAndSchemaPair.Schema, tableName)
				if err = ddl.DropTemporaryTable(store, fqTableName, true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
