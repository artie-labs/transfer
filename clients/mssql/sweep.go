package mssql

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/destination/ddl"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (s *Store) Sweep() error {
	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	// Find all the database and schema pairings
	// Then iterate over information schema
	// Find anything that has __artie__ in the table name, eval against `ShouldDeleteFromName`
	// If the table should be killed, it will drop it.
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	dbAndSchemaPairs := kafkalib.GetUniqueDatabaseAndSchema(tcs)
	for _, dbAndSchemaPair := range dbAndSchemaPairs {
		schema := getSchema(dbAndSchemaPair.Schema)

		query, args := sweepQuery(schema)
		rows, err := s.Store.Query(query, args...)
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableName string
			if err = rows.Scan(&tableName); err != nil {
				return err
			}

			if ddl.ShouldDeleteFromName(tableName) {
				if err = ddl.DropTemporaryTable(s, fmt.Sprintf("%s.%s", schema, tableName), true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
