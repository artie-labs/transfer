package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/retry"
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
			return fmt.Errorf("table %q does not exist in the in-memory database", args.SpecificTable)
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

			if args.CoolDown != nil && _tableData.ShouldSkipFlush(*args.CoolDown) {
				slog.Debug("Skipping flush because we are currently in a flush cooldown", slog.String("tableName", _tableName))
				return
			}

			action := "merge"
			if _tableData.Mode() == config.History {
				action = "append"
			}

			retryCfg, err := retry.NewJitterRetryConfig(500, 30_000, 10, retry.AlwaysRetry)
			if err != nil {
				slog.Error("Failed to create retry config", slog.Any("err", err))
				return
			}

			_tableData.Lock()
			defer _tableData.Unlock()
			err = retry.WithRetries(retryCfg, func(_ int, _ error) error {
				return flush(ctx, dest, metricsClient, args.Reason, _tableName, _tableData)
			})

			if err != nil {
				slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableName", _tableName))
			} else {
				inMemDB.ClearTableConfig(_tableName)
				slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableName", _tableName))
			}
		}(tableName, tableData)
	}
	wg.Wait()

	return nil
}

func flush(ctx context.Context, dest destination.Baseline, metricsClient base.Client, reason string, _tableName string, _tableData *models.TableData) error {
	if _tableData.Empty() {
		return nil
	}

	// This is added so that we have a new temporary table suffix for each merge / append.
	_tableData.ResetTempTableSuffix()

	start := time.Now()
	tags := map[string]string{
		"what":     "success",
		"mode":     _tableData.Mode().String(),
		"table":    _tableName,
		"database": _tableData.TopicConfig().Database,
		"schema":   _tableData.TopicConfig().Schema,
		"reason":   reason,
	}

	defer func() {
		metricsClient.Timing("flush", time.Since(start), tags)
	}()

	// Merge or Append depending on the mode.
	var err error
	if _tableData.Mode() == config.History {
		err = dest.Append(_tableData.TableData, false)
	} else {
		err = dest.Merge(_tableData.TableData)
	}

	if err != nil {
		tags["what"] = "merge_fail"
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err = commitOffset(ctx, _tableData.TopicConfig().Topic, _tableData.PartitionsToLastMessage); err != nil {
		// Failure to commit Kafka offset shouldn't force the whole flush process to retry.
		slog.Warn("Failed to commit Kafka offset", slog.Any("err", err), slog.String("tableName", _tableName))
	}

	return nil
}
