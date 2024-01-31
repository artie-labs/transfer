package main

import (
	"context"
	"fmt"
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
	"github.com/getsentry/sentry-go"
)

func main() {
	// Parse args into settings.
	ctx, err := config.InitializeCfgIntoContext(context.Background(), os.Args, true)
	if err != nil {
		logger.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	settings := config.FromContext(ctx)
	// Initialize default logger
	_logger, usingSentry := logger.NewLogger(settings.VerboseLogging, settings.Config.Reporting.Sentry)
	slog.SetDefault(_logger)
	if usingSentry {
		defer sentry.Flush(2 * time.Second)
		slog.Info("Sentry logger enabled")
	}

	// Loading Telemetry
	ctx = metrics.LoadExporter(ctx, settings.Config)
	if utils.IsOutputBaseline(settings.Config) {
		ctx = utils.InjectBaselineIntoCtx(utils.Baseline(settings.Config), ctx)
	} else {
		ctx = utils.InjectDwhIntoCtx(utils.DataWarehouse(settings.Config, nil), ctx)
	}

	ctx = models.LoadMemoryDB(ctx)
	slog.Info("Config is loaded",
		slog.Int("flushIntervalSeconds", settings.Config.FlushIntervalSeconds),
		slog.Uint64("bufferPoolSize", uint64(settings.Config.BufferRows)),
		slog.Int("flushPoolSizeKb", settings.Config.FlushSizeKb),
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
			consumer.StartConsumer(ctx, settings.Config)
		case constants.PubSub:
			consumer.StartSubscriber(ctx, settings.Config)
		default:
			logger.Fatal(fmt.Sprintf("Message queue: %s not supported", settings.Config.Queue))
		}
	}(ctx)

	wg.Wait()
}
