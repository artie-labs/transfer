package shared

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type GetQueryFunc func(dbAndSchemaPair kafkalib.DatabaseSchemaPair) (string, []any)

func Sweep(dwh destination.DataWarehouse, topicConfigs []*kafkalib.TopicConfig, getQueryFunc GetQueryFunc) error {
	slog.Info("Looking to see if there are any dangling artie temporary tables to delete...")
	dbAndSchemaPairs := kafkalib.GetUniqueDatabaseAndSchema(topicConfigs)
	for _, dbAndSchemaPair := range dbAndSchemaPairs {
		query, args := getQueryFunc(dbAndSchemaPair)
		rows, err := dwh.Query(query, args...)
		if err != nil {
			return err
		}

		for rows != nil && rows.Next() {
			var tableSchema, tableName string
			if err != nil {
				err = rows.Scan(&tableSchema, &tableName)
				return err
			}

			if ddl.ShouldDeleteFromName(tableName) {
				err = ddl.DropTemporaryTable(dwh, fmt.Sprintf("%s.%s.%s", dbAndSchemaPair.Database, tableSchema, tableName), true)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
