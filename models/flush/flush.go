package flush

import (
	"context"
	"github.com/artie-labs/transfer/lib/dwh/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/processes/consumer"
	"time"
)

func Flush(ctx context.Context) error {
	tables := FromContext(ctx).GetTables()
	log := logger.FromContext(ctx)

	for _, table := range tables {
		table.Lock()
		start := time.Now()
		logFields := map[string]interface{}{
			"tableName": table.Name,
		}

		tags := map[string]string{
			"what":     "success",
			"table":    table.Name,
			"database": table.TopicConfig.Database,
			"schema":   table.TopicConfig.Schema,
		}

		err := utils.FromContext(ctx).Merge(ctx, table.TableData)
		if err != nil {
			tags["what"] = "merge_fail"
			log.WithError(err).WithFields(logFields).Warn("Failed to execute merge...not going to flush memory")
		} else {
			log.WithFields(logFields).Info("Merge success, clearing memory...")
			commitErr := consumer.CommitOffset(ctx, table.TopicConfig.Topic, table.PartitionsToLastMessage)
			if commitErr == nil {
				FromContext(ctx).WipeTable(table.Name)
			} else {
				tags["what"] = "commit_fail"
				log.WithError(commitErr).Warn("commit error...")
			}
		}

		metrics.FromContext(ctx).Timing("flush", time.Since(start), tags)
		table.Unlock()
	}

	return nil
}
