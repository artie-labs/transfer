package snowflake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/artie-labs/transfer/lib/dwh/ddl"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
)

func (s *Store) Sweep(ctx context.Context) error {
	logger.FromContext(ctx).Info("looking to see if there are any dangling artie temporary tables to delete...")
	// Find all the database and schema pairings
	// Then iterate over information schema
	// Find anything that has __artie__ in the table name
	// Find the comment
	// If the table should be killed, it will drop it.
	tcs, err := config.FromContext(ctx).Config.TopicConfigs()
	if err != nil {
		return err
	}

	dbAndSchemaPairs := kafkalib.GetUniqueDatabaseAndSchema(tcs)
	for _, dbAndSchemaPair := range dbAndSchemaPairs {
		// ILIKE is used to be case-insensitive since Snowflake stores all the tables in UPPER.
		var rows *sql.Rows
		rows, err = s.Store.Query(fmt.Sprintf(
			`SELECT table_name, comment FROM %s.information_schema.tables where table_name ILIKE '%s' AND table_schema = UPPER('%s')`,
			dbAndSchemaPair.Database,
			"%"+constants.ArtiePrefix+"%", dbAndSchemaPair.Schema))
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
				err = ddl.DropTemporaryTable(ctx, s,
					fmt.Sprintf("%s.%s.%s", dbAndSchemaPair.Database, dbAndSchemaPair.Schema, tableName), true)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
