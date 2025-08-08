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
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

type Args struct {
	// [coolDown] - Is used to skip the flush if the table has been recently flushed.
	CoolDown *time.Duration
	// [specificTableID] - Is used to flush a specific table. If this is not set, we'll flush everything.
	SpecificTableID cdc.TableID
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
	allTables := inMemDB.TableData()
	inMemDB.RUnlock()

	if !args.SpecificTableID.IsEmpty() {
		if _, ok := allTables[args.SpecificTableID]; !ok {
			// Should never happen
			return fmt.Errorf("table %q does not exist in the in-memory database", args.SpecificTableID)
		}
	}

	// Flush will take everything in memory and call the destination to create temp tables.
	var wg sync.WaitGroup
	for tableID, tableData := range allTables {
		if !args.SpecificTableID.IsEmpty() && tableID != args.SpecificTableID {
			// If the table is specified within args and the table does not match the database, skip this flush.
			continue
		}

		wg.Add(1)
		go func(_tableID cdc.TableID, _tableData *models.TableData) {
			defer wg.Done()

			if args.CoolDown != nil && _tableData.ShouldSkipFlush(*args.CoolDown) {
				slog.Debug("Skipping flush because we are currently in a flush cooldown", slog.String("tableID", _tableID.String()))
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
				"table":    _tableID.Table,
				"database": _tableData.TopicConfig().Database,
				"schema":   _tableData.TopicConfig().Schema,
				"reason":   args.Reason,
			}

			what, err := retry.WithRetriesAndResult(retryCfg, func(_ int, _ error) (string, error) {
				return flush(ctx, dest, _tableData, _tableID, action, inMemDB.ClearTableConfig)
			})

			if err != nil {
				slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableID", _tableID.String()))
			}

			tags["what"] = what
			metricsClient.Timing("flush", time.Since(start), tags)
		}(tableID, tableData)
	}

	wg.Wait()
	return nil
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, _tableID cdc.TableID, action string, clearTableConfig func(cdc.TableID)) (string, error) {
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
		return "merge_fail", fmt.Errorf("failed to flush %q: %w", _tableID.String(), err)
	}

	if commitTransaction {
		provider, ok := kafkalib.GetTopicsToConsumerProviderFromContext(ctx)
		if !ok {
			return "commit_fail", fmt.Errorf("failed to get topics to consumer provider from context")
		}

		for _, msg := range _tableData.PartitionsToLastMessage {
			if err = provider.CommitMessage(ctx, _tableData.TopicConfig().Topic, msg.GetMessage()); err != nil {
				return "commit_fail", fmt.Errorf("failed to commit kafka offset: %w", err)
			}
		}

		slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableID.String()))
		clearTableConfig(_tableID)
	} else {
		slog.Info(fmt.Sprintf("%s success, not committing offset yet", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableID.String()))
	}

	return "success", nil
}
