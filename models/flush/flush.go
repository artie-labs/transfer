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

	// Create a channel where the buffer is the number of tables, so it doesn't block.
	flushChan := make(chan string, len(models.GetMemoryDB(ctx).TableData))

	// Flush will take everything in memory and call Snowflake to create temp tables.
	for tableName, tableData := range models.GetMemoryDB(ctx).TableData {
		wg.Add(1)
		go func(_tableName string, _tableData *optimization.TableData, flushChan chan string) {
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
					flushChan <- _tableName
				} else {
					tags["what"] = "commit_fail"
					log.WithError(commitErr).Warn("commit error...")
				}
			}
			metrics.FromContext(ctx).Timing("flush", time.Since(start), tags)
		}(tableName, tableData, flushChan)

	}
	wg.Wait()

	// Close the channel so no more rows can be added.
	close(flushChan)

	for tableName := range flushChan {
		// Now drain the channel.
		models.GetMemoryDB(ctx).ClearTableConfig(tableName)
	}

	return nil
}
