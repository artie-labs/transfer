package main

import (
	"context"
	"os"
	"sync"

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
	config.ParseArgs(os.Args, true)
	ctx := logger.InjectLoggerIntoCtx(logger.NewLogger(config.GetSettings()), context.Background())

	// Loading Telemetry
	ctx = metrics.LoadExporter(ctx, config.GetSettings().Config.Telemetry.Metrics.Provider,
		config.GetSettings().Config.Telemetry.Metrics.Settings)
	ctx = utils.InjectDwhIntoCtx(utils.DataWarehouse(ctx), ctx)

	models.LoadMemoryDB()

	flushChan := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, constants.FlushTimeInterval, flushChan)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		switch config.GetSettings().Config.Queue {
		case constants.Kafka:
			consumer.StartConsumer(ctx, flushChan)
			break
		case constants.PubSub:
			consumer.StartSubscriber(ctx, flushChan)
			break
		default:
			logger.FromContext(ctx).Fatalf("message queue: %s not supported", config.GetSettings().Config.Queue)
		}

	}(ctx)

	wg.Wait()
}
