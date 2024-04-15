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
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	// Initialize default logger
	_logger, cleanUpHandlers := logger.NewLogger(settings.VerboseLogging, settings.Config.Reporting.Sentry)
	slog.SetDefault(_logger)

	defer cleanUpHandlers()

	slog.Info("Config is loaded",
		slog.Int("flushIntervalSeconds", settings.Config.FlushIntervalSeconds),
		slog.Uint64("bufferPoolSize", uint64(settings.Config.BufferRows)),
		slog.Int("flushPoolSizeKb", settings.Config.FlushSizeKb),
	)

	ctx := context.Background()

	// Loading telemetry
	metricsClient := metrics.LoadExporter(settings.Config)
	var dest destination.Baseline
	if utils.IsOutputBaseline(settings.Config) {
		dest, err = utils.LoadBaseline(settings.Config)
		if err != nil {
			logger.Fatal("Unable to load baseline destination", slog.Any("err", err))
		}
	} else {
		dest, err = utils.LoadDataWarehouse(settings.Config, nil)
		if err != nil {
			logger.Fatal("Unable to load data warehouse destination", slog.Any("err", err))
		}
	}

	inMemDB := models.NewMemoryDB()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, inMemDB, dest, metricsClient, time.Duration(settings.Config.FlushIntervalSeconds)*time.Second)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartConsumer(ctx, settings.Config, inMemDB, dest, metricsClient)
		case constants.PubSub:
			consumer.StartSubscriber(ctx, settings.Config, inMemDB, dest, metricsClient)
		default:
			logger.Fatal(fmt.Sprintf("Message queue: %s not supported", settings.Config.Queue))
		}
	}(ctx)

	wg.Wait()
}
