package flush

import (
	"context"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/kafka"
)

func Flush(ctx context.Context) error {
	if models.InMemoryDB == nil {
		return nil
	}

	log := logger.FromContext(ctx)
	models.InMemoryDB.Lock()
	defer models.InMemoryDB.Unlock()

	// Flush will take everything in memory and call Snowflake to create temp tables.
	for tableName, tableData := range models.InMemoryDB.TableData {
		logFields := map[string]interface{}{
			"tableName": tableName,
		}

		err := snowflake.ExecuteMerge(ctx, tableData)
		if err != nil {
			log.WithError(err).WithFields(logFields).Warn("Failed to execute merge...not going to flush memory")
		} else {
			log.WithFields(logFields).Info("Merge success, clearing memory...")
			commitErr := kafka.CommitOffset(tableData.Topic, tableData.PartitionsToOffset)
			if commitErr == nil {
				models.InMemoryDB.ClearTableConfig(tableName)
			} else {
				log.WithError(commitErr).Warn("commit error...")
			}
		}
	}

	return nil
}
