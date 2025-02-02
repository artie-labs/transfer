package shared

import (
	"log/slog"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type GetQueryFunc func(dbName string, schemaName string) (string, []any)

func Sweep(dwh destination.Destination, topicConfigs []*kafkalib.TopicConfig, getQueryFunc GetQueryFunc) error {
	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	for _, topicConfig := range kafkalib.GetUniqueTopicConfigs(topicConfigs) {
		query, args := getQueryFunc(topicConfig.Database, topicConfig.Schema)
		rows, err := dwh.Query(query, args...)
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
				err = ddl.DropTemporaryTable(dwh, dwh.IdentifierFor(topicConfig, tableName), true)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
