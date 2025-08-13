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
	// [Topic] - This is the specific topic that you would like to flush.
	Topic string
	// [reason] - Is used to track the reason for the flush.
	Reason string

	// [ShouldLock] - If this is set to true, we will lock the consumer for the duration of the flush
	ShouldLock bool
}

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

	topicsToConsumerProvider := make(map[string]*kafkalib.ConsumerProvider)
	for topic := range topicToTables {
		consumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
		if err != nil {
			return fmt.Errorf("failed to get consumer from context: %w", err)
		}

		topicsToConsumerProvider[topic] = consumer
	}

	// Flush will take everything in memory and call the destination to create temp tables.
	var wg sync.WaitGroup
	for topic, tables := range topicToTables {
		if args.Topic != "" && args.Topic != topic {
			// If topic was specified and doesn't match this topic, we'll skip flushing this topic.
			continue
		}

		consumer, ok := topicsToConsumerProvider[topic]
		if !ok {
			return fmt.Errorf("consumer not found for topic %q", topic)
		}
		fmt.Println("Trying to flush now", args.Topic)
		consumer.LockAndProcess(ctx, args.ShouldLock, func() error {
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
						return flush(ctx, dest, _tableData, action, inMemDB.ClearTableConfig, consumer)
					})

					if err != nil {
						slog.Error(fmt.Sprintf("Failed to %s", action), slog.Any("err", err), slog.String("tableID", _tableData.GetTableID().String()))
					}

					tags["what"] = what
					metricsClient.Timing("flush", time.Since(start), tags)
				}(tableData)
			}

			return nil
		})
	}

	wg.Wait()
	return nil
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, action string, clearTableConfig func(cdc.TableID), consumer *kafkalib.ConsumerProvider) (string, error) {
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
		}

		slog.Info(fmt.Sprintf("%s success, clearing memory...", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
		clearTableConfig(_tableData.GetTableID())
	} else {
		slog.Info(fmt.Sprintf("%s success, not committing offset yet", stringutil.CapitalizeFirstLetter(action)), slog.String("tableID", _tableData.GetTableID().String()))
	}

	return "success", nil
}
