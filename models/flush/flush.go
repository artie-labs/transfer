package flush

import (
	"context"
	"github.com/artie-labs/transfer/lib/dwh/utils"
	"time"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/kafka"
)

func Flush(ctx context.Context) error {
	if models.GetMemoryDB() == nil {
		return nil
	}

	log := logger.FromContext(ctx)
	models.GetMemoryDB().Lock()
	defer models.GetMemoryDB().Unlock()

	// Flush will take everything in memory and call Snowflake to create temp tables.
	for tableName, tableData := range models.GetMemoryDB().TableData {
		start := time.Now()
		logFields := map[string]interface{}{
			"tableName": tableName,
		}

		tags := map[string]string{
			"what":     "success",
			"table":    tableName,
			"database": tableData.Database,
			"schema":   tableData.Schema,
		}

		err := utils.FromContext(ctx).Merge(ctx, tableData)
		if err != nil {
			tags["what"] = "merge_fail"
			log.WithError(err).WithFields(logFields).Warn("Failed to execute merge...not going to flush memory")

		} else {
			log.WithFields(logFields).Info("Merge success, clearing memory...")
			commitErr := kafka.CommitOffset(ctx, tableData.Topic, tableData.PartitionsToLastMessage)
			if commitErr == nil {
				models.GetMemoryDB().ClearTableConfig(tableName)
			} else {
				tags["what"] = "commit_fail"
				log.WithError(commitErr).Warn("commit error...")
			}
		}

		metrics.FromContext(ctx).Timing("flush", time.Since(start), tags)
	}

	return nil
}
