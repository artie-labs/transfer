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
				"table":    _tableName,
				"database": _tableData.TopicConfig().Database,
				"schema":   _tableData.TopicConfig().Schema,
				"reason":   args.Reason,
			}

			what, err := retry.WithRetriesAndResult(retryCfg, func(_ int, _ error) (string, error) {
				return flush(ctx, dest, _tableData, inMemDB, _tableName, action)
			})

			if err != nil {
				slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableName", _tableName))
			}

			tags["what"] = what
			metricsClient.Timing("flush", time.Since(start), tags)
		}(tableName, tableData)
	}

	wg.Wait()
	return nil
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, inMemDB *models.DatabaseData, _tableName string, action string) (string, error) {
	// This is added so that we have a new temporary table suffix for each merge / append.
	_tableData.ResetTempTableSuffix()

	// Merge or Append depending on the mode.
	var err error
	var commit bool
	if _tableData.Mode() == config.History {
		// Always commit on append if it's successful
		err = dest.Append(ctx, _tableData.TableData, false)
		commit = true
	} else {
		commit, err = dest.Merge(ctx, _tableData.TableData)
	}

	if err != nil {
		return "merge_fail", fmt.Errorf("failed to flush: %w", err)
	}

	if commit {
		if err = commitOffset(ctx, _tableData.TopicConfig().Topic, _tableData.PartitionsToLastMessage); err != nil {
			return "commit_fail", fmt.Errorf("failed to commit kafka offset: %w", err)
		}

		slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableName", _tableName))
		inMemDB.ClearTableConfig(_tableName)
	}

	return "success", nil
}
