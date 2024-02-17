package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

type Args struct {
	// If cooldown is passed in, we'll skip the flush if the table has been recently flushed
	CoolDown *time.Duration
	// If specificTable is not passed in, we'll just flush everything.
	SpecificTable string

	// Reason (reason for the flush)
	Reason string
}

// Flush will merge/append and commit the offset on the specified topics within `args.SpecificTable`.
// If the table list is empty, it'll flush everything. This is the default behavior for the time duration based flush.
// Table specific flushes will be triggered based on the size of the pool (length and size wise).
func Flush(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, args Args) error {
	if inMemDB == nil {
		return nil
	}

	// Read lock to examine the map of tables
	inMemDB.RLock()
	allTables := inMemDB.TableData()
	inMemDB.RUnlock()

	if args.SpecificTable != "" {
		if _, ok := allTables[args.SpecificTable]; !ok {
			// Should never happen
			return fmt.Errorf("table %s does not exist in the in-memory database", args.SpecificTable)
		}
	}

	// Flush will take everything in memory and call the destination to create temp tables.
	var wg sync.WaitGroup
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

			if args.CoolDown != nil && _tableData.ShouldSkipFlush(*args.CoolDown) {
				slog.With(logFields...).Info("Skipping flush because we are currently in a flush cooldown")
				return
			}

			// Lock the tables when executing merge / append.
			_tableData.Lock()
			defer _tableData.Unlock()
			if _tableData.Empty() {
				return
			}

			// This is added so that we have a new temporary table suffix for each merge / append.
			_tableData.ResetTempTableSuffix()

			start := time.Now()
			tags := map[string]string{
				"what":     "success",
				"mode":     _tableData.Mode().String(),
				"table":    _tableName,
				"database": _tableData.TopicConfig.Database,
				"schema":   _tableData.TopicConfig.Schema,
				"reason":   args.Reason,
			}

			var err error
			action := "merge"
			// Merge or Append depending on the mode.
			if _tableData.Mode() == config.History {
				err = dest.Append(_tableData.TableData)
				action = "append"
			} else {
				err = dest.Merge(_tableData.TableData)
			}

			if err != nil {
				tags["what"] = "merge_fail"
				tags["retryable"] = fmt.Sprint(dest.IsRetryableError(err))
				slog.With(logFields...).Error(fmt.Sprintf("Failed to execute %s, not going to flush memory, will sleep for 3 seconds before continuing...", action), slog.Any("err", err))
				time.Sleep(3 * time.Second)
			} else {
				slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), logFields...)
				commitErr := commitOffset(ctx, _tableData.TopicConfig.Topic, _tableData.PartitionsToLastMessage)
				if commitErr == nil {
					inMemDB.ClearTableConfig(_tableName)
				} else {
					tags["what"] = "commit_fail"
					slog.Warn("Commit error...", slog.Any("err", commitErr))
				}
			}
			metricsClient.Timing("flush", time.Since(start), tags)
		}(tableName, tableData)
	}
	wg.Wait()

	return nil
}
