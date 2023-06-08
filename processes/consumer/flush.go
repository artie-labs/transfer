package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/dwh/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
)

type Args struct {
	Context context.Context
	// If specificTable is not passed in, we'll just flush everything.
	SpecificTable string
}

func Flush(args Args) error {
	if models.GetMemoryDB(args.Context) == nil {
		return nil
	}

	log := logger.FromContext(args.Context)
	var wg sync.WaitGroup
	// Read lock to examine the map of tables
	models.GetMemoryDB(args.Context).RLock()
	allTables := models.GetMemoryDB(args.Context).TableData()
	models.GetMemoryDB(args.Context).RUnlock()

	// Flush will take everything in memory and call Snowflake to create temp tables.
	for tableName, tableData := range allTables {
		if args.SpecificTable != "" && tableName != args.SpecificTable {
			// If the table is specified within args and the table does not match the database, skip this flush.
			continue
		}

		wg.Add(1)
		go func(_tableName string, _tableData *models.TableData) {
			// Lock the tables when executing merge.
			_tableData.Lock()
			defer _tableData.Unlock()
			defer wg.Done()

			if _tableData.Empty() {
				return
			}

			start := time.Now()
			logFields := map[string]interface{}{
				"tableName": _tableName,
			}

			tags := map[string]string{
				"what":     "success",
				"table":    _tableName,
				"database": _tableData.TopicConfig.Database,
				"schema":   _tableData.TopicConfig.Schema,
			}

			err := utils.FromContext(args.Context).Merge(args.Context, _tableData.TableData)
			if err != nil {
				tags["what"] = "merge_fail"
				log.WithError(err).WithFields(logFields).Warn("Failed to execute merge...not going to flush memory")
			} else {
				log.WithFields(logFields).Info("Merge success, clearing memory...")
				commitErr := CommitOffset(args.Context, _tableData.TopicConfig.Topic, _tableData.PartitionsToLastMessage)
				if commitErr == nil {
					models.GetMemoryDB(args.Context).ClearTableConfig(_tableName)
				} else {
					tags["what"] = "commit_fail"
					log.WithError(commitErr).Warn("commit error...")
				}
			}
			metrics.FromContext(args.Context).Timing("flush", time.Since(start), tags)

		}(tableName, tableData)
	}
	wg.Wait()

	return nil
}
