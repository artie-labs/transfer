package flush

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/dwh/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
)

func Flush(ctx context.Context) error {
	if models.GetMemoryDB(ctx) == nil {
		return nil
	}

	log := logger.FromContext(ctx)
	models.GetMemoryDB(ctx).Lock()
	defer models.GetMemoryDB(ctx).Unlock()

	var wg sync.WaitGroup
	// Flush will take everything in memory and call Snowflake to create temp tables.
	for tableName, tableData := range models.GetMemoryDB(ctx).TableData {
		wg.Add(1)

		go func(_tableName string, _tableData *optimization.TableData) {
			defer wg.Done()
			start := time.Now()
			logFields := map[string]interface{}{
				"tableName": _tableName,
			}

			tags := map[string]string{
				"what":     "success",
				"table":    _tableName,
				"database": _tableData.Database,
				"schema":   _tableData.Schema,
			}

			err := utils.FromContext(ctx).Merge(ctx, _tableData)
			if err != nil {
				tags["what"] = "merge_fail"
				log.WithError(err).WithFields(logFields).Warn("Failed to execute merge...not going to flush memory")
			} else {
				log.WithFields(logFields).Info("Merge success, clearing memory...")
				commitErr := consumer.CommitOffset(ctx, _tableData.Topic, _tableData.PartitionsToLastMessage)
				if commitErr == nil {
					models.GetMemoryDB(ctx).ClearTableConfig(_tableName)
				} else {
					tags["what"] = "commit_fail"
					log.WithError(commitErr).Warn("commit error...")
				}
			}
			metrics.FromContext(ctx).Timing("flush", time.Since(start), tags)
		}(tableName, tableData)

		wg.Wait()
	}

	return nil
}
