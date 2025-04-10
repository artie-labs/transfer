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
	for _, topicConfig := range kafkalib.GetUniqueDatabaseAndSchemaPairs(topicConfigs) {
		query, args := getQueryFunc(topicConfig.Database, topicConfig.Schema)
		rows, err := dest.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableSchema, tableName string
			err = rows.Scan(&tableSchema, &tableName)
			if err != nil {
				return err
			}

			if ddl.ShouldDeleteFromName(tableName) {
				err = ddl.DropTemporaryTable(ctx, dest, dest.IdentifierFor(topicConfig, tableName), true)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
