package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

type Args struct {
	// [coolDown] - Is used to skip the flush if the table has been recently flushed.
	CoolDown *time.Duration
	// [Topic] - This is the specific topic that you would like to flush.
	Topic string
	// [reason] - Is used to track the reason for the flush.
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
	topicToTables := inMemDB.GetTopicToTables()
	inMemDB.RUnlock()

	if args.Topic != "" {
		if _, ok := topicToTables[args.Topic]; !ok {
			// Should never happen
			return fmt.Errorf("topic %q does not exist in the in-memory database", args.Topic)
		}
	}

	// Flush will take everything in memory and call the destination to create temp tables.
	var wg sync.WaitGroup
	for topic, tables := range topicToTables {
		if args.Topic != "" && args.Topic != topic {
			// If topic was specified and doesn't match this topic, we'll skip flushing this topic.
			continue
		}

		for _, tableData := range tables {
			wg.Add(1)
			go func(_tableData *models.TableData) {
				defer wg.Done()

				if args.CoolDown != nil && _tableData.ShouldSkipFlush(*args.CoolDown) {
					slog.Debug("Skipping flush because we are currently in a flush cooldown", slog.String("tableID", _tableData.GetTableID().String()))
					return
				}

				retryCfg, err := retry.NewJitterRetryConfig(1_000, 30_000, 15, retry.AlwaysRetry)
				if err != nil {
					slog.Error("Failed to create retry config", slog.Any("err", err))
					return
				}

				_tableData.Lock()
				defer _tableData.Unlock()
				if _tableData.Empty() {
					return
				}

				action := "merge"
				if _tableData.Mode() == config.History {
					action = "append"
				}

				start := time.Now()
				tags := map[string]string{
					"mode":     _tableData.Mode().String(),
					"table":    _tableData.GetTableID().Table,
					"database": _tableData.TopicConfig().Database,
					"schema":   _tableData.TopicConfig().Schema,
					"reason":   args.Reason,
				}

				what, err := retry.WithRetriesAndResult(retryCfg, func(_ int, _ error) (string, error) {
					return flush(ctx, dest, _tableData, action, inMemDB.ClearTableConfig)
				})

				if err != nil {
					slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableID", _tableData.GetTableID().String()))
				}

				tags["what"] = what
				metricsClient.Timing("flush", time.Since(start), tags)
			}(tableData)
		}
	}

	wg.Wait()
	return nil
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, action string, clearTableConfig func(cdc.TableID)) (string, error) {
	// This is added so that we have a new temporary table suffix for each merge / append.
	_tableData.ResetTempTableSuffix()

	// Merge or Append depending on the mode.
	var err error
	commitTransaction := true
	if _tableData.Mode() == config.History {
		err = dest.Append(ctx, _tableData.TableData, false)
	} else {
		commitTransaction, err = dest.Merge(ctx, _tableData.TableData)
	}

	if err != nil {
		return "merge_fail", fmt.Errorf("failed to flush %q: %w", _tableData.GetTableID().String(), err)
	}

	if commitTransaction {
		if err = commitOffset(ctx, _tableData.TopicConfig().Topic, _tableData.PartitionsToLastMessage); err != nil {
			return "commit_fail", fmt.Errorf("failed to commit kafka offset: %w", err)
		}

		slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
		clearTableConfig(_tableData.GetTableID())
	} else {
		slog.Info(fmt.Sprintf("%s success, not committing offset yet", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
	}

	return "success", nil
}
