package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings.
	ctx := config.InitializeCfgIntoContext(context.Background(), os.Args, true)

	// Initialize default logger
	slog.SetDefault(logger.NewLogger(config.FromContext(ctx)))

	// Loading Telemetry
	ctx = metrics.LoadExporter(ctx)
	if utils.IsOutputBaseline(ctx) {
		ctx = utils.InjectBaselineIntoCtx(utils.Baseline(ctx), ctx)
	} else {
		ctx = utils.InjectDwhIntoCtx(utils.DataWarehouse(ctx, nil), ctx)
	}

	ctx = models.LoadMemoryDB(ctx)
	settings := config.FromContext(ctx)

	slog.Info("config is loaded",
		slog.Int("flush_interval_seconds", settings.Config.FlushIntervalSeconds),
		slog.Uint64("buffer_pool_size", uint64(settings.Config.BufferRows)),
		slog.Int("flush_pool_size (kb)", settings.Config.FlushSizeKb),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, time.Duration(settings.Config.FlushIntervalSeconds)*time.Second)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartConsumer(ctx)
		case constants.PubSub:
			consumer.StartSubscriber(ctx)
		default:
			logger.Fatalf("message queue: %s not supported", settings.Config.Queue)
		}
	}(ctx)

	wg.Wait()
}
