package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
)

type Args struct {
	Context context.Context
	// If cooldown is passed in, we'll skip the merge if the table has been recently merged
	CoolDown *time.Duration
	// If specificTable is not passed in, we'll just flush everything.
	SpecificTable string

	// Reason (reason for the flush)
	Reason string
}

// Flush will merge and commit the offset on the specified topics within `args.SpecificTable`.
// If the table list is empty, it'll flush everything. This is the default behavior for the time duration based flush.
// Table specific flushes will be triggered based on the size of the pool (length and size wise).
func Flush(args Args) error {
	if models.GetMemoryDB(args.Context) == nil {
		return nil
	}

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
			defer wg.Done()

			logFields := []any{
				slog.String("tableName", _tableName),
			}

			if args.CoolDown != nil && _tableData.ShouldSkipMerge(*args.CoolDown) {
				slog.With(logFields...).Info("skipping merge because we are currently in a merge cooldown")
				return
			}

			// Lock the tables when executing merge.
			_tableData.Lock()
			defer _tableData.Unlock()
			if _tableData.Empty() {
				return
			}

			// This is added so that we have a new temporary table suffix for each merge.
			_tableData.ResetTempTableSuffix()

			start := time.Now()
			tags := map[string]string{
				"what":     "success",
				"table":    _tableName,
				"database": _tableData.TopicConfig.Database,
				"schema":   _tableData.TopicConfig.Schema,
				"reason":   args.Reason,
			}

			err := utils.FromContext(args.Context).Merge(args.Context, _tableData.TableData)
			if err != nil {
				tags["what"] = "merge_fail"
				slog.With(logFields...).Warn("Failed to execute merge...not going to flush memory", slog.Any("err", err))
			} else {
				slog.With(logFields...).Info("Merge success, clearing memory...")
				commitErr := commitOffset(args.Context, _tableData.TopicConfig.Topic, _tableData.PartitionsToLastMessage)
				if commitErr == nil {
					models.GetMemoryDB(args.Context).ClearTableConfig(_tableName)
				} else {
					tags["what"] = "commit_fail"
					slog.Warn("commit error...", slog.Any("err", commitErr))
				}
			}
			metrics.FromContext(args.Context).Timing("flush", time.Since(start), tags)
		}(tableName, tableData)
	}
	wg.Wait()

	return nil
}
