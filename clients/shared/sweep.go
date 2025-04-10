package shared

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type GetQueryFunc func(dbName string, schemaName string) (string, []any)

func Sweep(ctx context.Context, dest destination.Destination, topicConfigs []*kafkalib.TopicConfig, getQueryFunc GetQueryFunc) error {
	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	for _, dbAndSchemaPair := range kafkalib.GetUniqueDatabaseAndSchemaPairs(topicConfigs) {
		query, args := getQueryFunc(dbAndSchemaPair.Database, dbAndSchemaPair.Schema)
		rows, err := dest.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableSchema, tableName string
			if err = rows.Scan(&tableSchema, &tableName); err != nil {
				return err
			}

			if ddl.ShouldDeleteFromName(tableName) {
				if err = ddl.DropTemporaryTable(ctx, dest, dest.IdentifierFor(dbAndSchemaPair, tableName), true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
