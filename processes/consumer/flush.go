package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
	"golang.org/x/sync/errgroup"
)

type Args struct {
	// [coolDown] - Is used to skip the flush if the table has been recently flushed.
	CoolDown *time.Duration
	// [reason] - Is used to track the reason for the flush.
	Reason string
}

func Flush(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, topics []string, args Args) error {
	if inMemDB == nil {
		return nil
	}

	for _, topic := range topics {
		if err := FlushSingleTopic(ctx, inMemDB, dest, metricsClient, args, topic, true); err != nil {
			slog.Error("Failed to flush topic", slog.String("topic", topic), slog.Any("err", err))
		}
	}

	return nil
}

func FlushSingleTopic(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, args Args, topic string, shouldLock bool) error {
	if inMemDB == nil {
		return nil
	}

	tables := inMemDB.GetTables(topic)
	if len(tables) == 0 {
		return nil
	}

	consumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get consumer from context: %w", err)
	}

	var grp errgroup.Group
	var commitOffset bool
	err = consumer.LockAndProcess(ctx, shouldLock, func() error {
		for _, table := range tables {
			// Also in the example: https://pkg.go.dev/golang.org/x/sync/errgroup#example-Group-Parallel
			table := table // https://golang.org/doc/faq#closures_and_goroutines
			grp.Go(func() error {
				if args.CoolDown != nil && table.ShouldSkipFlush(*args.CoolDown) {
					slog.Debug("Skipping flush because we are currently in a flush cooldown", slog.String("tableID", table.GetTableID().String()))
					return nil
				}

				retryCfg, err := retry.NewJitterRetryConfig(1_000, 30_000, 15, retry.AlwaysRetry)
				if err != nil {
					return err
				}

				if table.Empty() {
					return nil
				}

				action := "merge"
				if table.Mode() == config.History {
					action = "append"
				}

				start := time.Now()
				tags := map[string]string{
					"mode":     table.Mode().String(),
					"table":    table.GetTableID().Table,
					"database": table.TopicConfig().Database,
					"schema":   table.TopicConfig().Schema,
					"reason":   args.Reason,
				}

				result, err := retry.WithRetriesAndResult(retryCfg, func(_ int, _ error) (flushResult, error) {
					slog.Info("Flushing table", slog.String("tableID", table.GetTableID().String()), slog.String("reason", args.Reason))
					return flush(ctx, dest, table, action, consumer)
				})

				if err != nil {
					return fmt.Errorf("failed to %s for %q: %w", action, table.GetTableID().String(), err)
				}

				// It's okay that this will get overwritten by other tables
				// This is because MSM is only supported for a single table / topic.
				commitOffset = result.CommitOffset
				tags["what"] = result.What
				metricsClient.Timing("flush", time.Since(start), tags)
				return nil
			})
		}

		if err = grp.Wait(); err != nil {
			return fmt.Errorf("failed to flush table: %w", err)
		}

		if commitOffset {
			if err := consumer.CommitMessage(ctx); err != nil {
				return fmt.Errorf("failed to commit message: %w", err)
			}

			for _, table := range tables {
				inMemDB.ClearTableConfig(table.GetTableID())
			}
		} else {
			slog.Info("Not committing offset yet", slog.String("topic", topic))
		}

		return nil
	})

	return err
}

type flushResult struct {
	What         string
	CommitOffset bool
}

func flush(ctx context.Context, dest destination.Baseline, _tableData *models.TableData, action string, consumer *kafkalib.ConsumerProvider) (flushResult, error) {
	// This is added so that we have a new temporary table suffix for each merge / append.
	_tableData.ResetTempTableSuffix()

	// Merge or Append depending on the mode.
	switch _tableData.Mode() {
	case config.History:
		err := dest.Append(ctx, _tableData.TableData, false)
		if err != nil {
			return flushResult{What: "merge_fail"}, fmt.Errorf("failed to append: %w", err)
		}

		return flushResult{What: "success", CommitOffset: true}, nil
	case config.Replication:
		commitTransaction, err := dest.Merge(ctx, _tableData.TableData)
		if err != nil {
			return flushResult{What: "merge_fail"}, fmt.Errorf("failed to merge: %w", err)
		}

		return flushResult{What: "success", CommitOffset: commitTransaction}, nil
	}

	return flushResult{}, fmt.Errorf("invalid mode: %q", _tableData.Mode())
}
