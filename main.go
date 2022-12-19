package main

import (
	"context"
	"github.com/artie-labs/transfer/lib/metrics/stats"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/kafka"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings.
	config.ParseArgs(os.Args, true)
	ctx := logger.InjectLoggerIntoCtx(logger.NewLogger(config.GetSettings()), context.Background())

	// Loading Telemetry
	stats.LoadExporter(ctx, config.GetSettings().Config.Telemetry.Metrics.Provider,
		config.GetSettings().Config.Telemetry.Metrics.Settings)

	// Loading the destination
	if config.GetSettings().Config.Output == "test" {
		store := db.Store(&mock.DB{
			Fake: mocks.FakeStore{},
		})
		snowflake.LoadSnowflake(ctx, &store)
	} else {
		snowflake.LoadSnowflake(ctx, nil)
	}

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
