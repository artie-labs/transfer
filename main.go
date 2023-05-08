package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings.
	ctx := config.InitializeCfgIntoContext(context.Background(), os.Args, true)
	ctx = logger.InjectLoggerIntoCtx(ctx)

	// Loading Telemetry
	ctx = metrics.LoadExporter(ctx)
	ctx = utils.InjectDwhIntoCtx(utils.DataWarehouse(ctx, nil), ctx)

	models.LoadMemoryDB()
	settings := config.FromContext(ctx)
	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"flush_interval_seconds": settings.Config.FlushIntervalSeconds,
		"buffer_pool_size":       settings.Config.BufferRows,
		"flush_pool_size (kb)": settings.Config.FlushSizeKb,
	}).Info("config is loaded")

	flushChan := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, time.Duration(settings.Config.FlushIntervalSeconds)*time.Second, flushChan)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartConsumer(ctx, flushChan)
			break
		case constants.PubSub:
			consumer.StartSubscriber(ctx, flushChan)
			break
		default:
			logger.FromContext(ctx).Fatalf("message queue: %s not supported", settings.Config.Queue)
		}

	}(ctx)

	wg.Wait()
}
