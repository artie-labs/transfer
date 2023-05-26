package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/dwh/ddl"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
)

func shouldDelete(comment string) (shouldDelete bool) {
	// expires:2023-05-26 05:57:48 UTC
	if strings.HasPrefix(comment, constants.SnowflakeExpireCommentPrefix) {
		trimmedComment := strings.TrimPrefix(comment, constants.SnowflakeExpireCommentPrefix)
		ts, err := typing.FromExpiresDateStringToTime(trimmedComment)
		if err != nil {
			return false
		}

		// We should delete it if the time right now is AFTER the ts in the comment.
		return time.Now().After(ts)
	}

	return false
}

func (s *Store) Sweep(ctx context.Context) error {
	if !s.useStaging {
		return nil
	}

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

			if shouldDelete(comment) {
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
