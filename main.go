package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/kafka"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings.
	config.ParseArgs(os.Args, true)
	ctx := logger.InjectLoggerIntoCtx(logger.NewLogger(config.GetSettings()), context.Background())

	// Loading Telemetry
	ctx = metrics.LoadExporter(ctx, config.GetSettings().Config.Telemetry.Metrics.Provider,
		config.GetSettings().Config.Telemetry.Metrics.Settings)

	ctx = dwh.InjectDwhIntoCtx(dwh.LoadDataWarehouse(ctx), ctx)

	models.LoadMemoryDB()

	flushChan := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, 10*time.Second, flushChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		kafka.StartConsumer(ctx, flushChan)
	}()

	wg.Wait()
}
