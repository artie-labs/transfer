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
	// [reason] - Is used to track the reason for the flush.
	Reason string
	// [ShouldLock] - If this is set to true, we will lock the consumer for the duration of the flush
	ShouldLock bool
}

func Flush(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, args Args) error {
	if inMemDB == nil {
		return nil
	}

	var wg sync.WaitGroup
	for _, topic := range inMemDB.GetTopics() {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			if err := FlushTopic(ctx, inMemDB, dest, metricsClient, topic, args); err != nil {
				slog.Error("Failed to flush topic", slog.String("topic", topic), slog.Any("err", err))
			}
		}(topic)
	}

	wg.Wait()
	return nil
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, action string, clearTableConfig func(string, cdc.TableID), consumer *kafkalib.ConsumerProvider) (string, error) {
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
		for _, msg := range _tableData.PartitionsToLastMessage {
			if err = consumer.CommitMessage(ctx, msg.GetMessage()); err != nil {
				return "commit_fail", fmt.Errorf("failed to commit kafka offset: %w", err)
			}

			slog.Info("Successfully committed Kafka offset", slog.String("topic", msg.Topic()), slog.Int("partition", msg.Partition()), slog.Int64("offset", msg.Offset()))
		}

		slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
		clearTableConfig(_tableData.Topic(), _tableData.GetTableID())
	} else {
		slog.Info(fmt.Sprintf("%s success, not committing offset yet", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
	}

	return "success", nil
}

func FlushTopic(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, topic string, args Args) error {
	consumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get consumer from context: %w", err)
	}

	return consumer.LockAndProcess(ctx, args.ShouldLock, func() error {
		// 1. Fetch all the tables for this topic.
		tables := inMemDB.GetTablesForTopic(topic)

		// 2. Flush each table.
		var tableWg sync.WaitGroup
		for _, table := range tables {
			tableWg.Add(1)
			go func(_tableData *models.TableData) {
				defer tableWg.Done()
				if args.CoolDown != nil && _tableData.ShouldSkipFlush(*args.CoolDown) {
					slog.Debug("Skipping flush because we are currently in a flush cooldown", slog.String("tableID", _tableData.GetTableID().String()))
					return
				}

				retryCfg, err := retry.NewJitterRetryConfig(1_000, 30_000, 15, retry.AlwaysRetry)
				if err != nil {
					slog.Error("Failed to create retry config", slog.Any("err", err))
					return
				}

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
					slog.Info("Flushing table", slog.String("tableID", _tableData.GetTableID().String()), slog.String("reason", args.Reason))
					return flush(ctx, dest, _tableData, action, inMemDB.ClearTableConfig, consumer)
				})

				if err != nil {
					slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableID", _tableData.GetTableID().String()))
				}

				tags["what"] = what
				metricsClient.Timing("flush", time.Since(start), tags)
			}(table)
		}

		tableWg.Wait()
		return nil
	})
}
