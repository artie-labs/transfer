package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/artie-labs/transfer/processes/pool"
	"github.com/segmentio/kafka-go"
)

var (
	version = "dev" // this will be set by the goreleaser configuration to appropriate value for the compiled binary.
)

func main() {
	// Parse args into settings
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	// Initialize default logger
	_logger, cleanUpHandlers := logger.NewLogger(settings.VerboseLogging, settings.Config.Reporting.Sentry, version)
	slog.SetDefault(_logger)

	defer cleanUpHandlers()

	// This is used to prevent all the instances from starting at the same time and causing a thundering herd problem
	if value := os.Getenv("MAX_INIT_SLEEP_SECONDS"); value != "" {
		castedValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			logger.Fatal("Failed to parse sleep duration", slog.Any("err", err), slog.String("value", value))
		}

		randomSeconds, err := cryptography.RandomInt64n(castedValue)
		if err != nil {
			logger.Fatal("Failed to generate random number", slog.Any("err", err))
		}

		duration := time.Duration(randomSeconds) * time.Second
		slog.Info(fmt.Sprintf("Sleeping for %s before any data processing to prevent overwhelming Kafka", duration.String()))
		time.Sleep(duration)
	}

	slog.Info("Config is loaded",
		slog.Int("flushIntervalSeconds", settings.Config.FlushIntervalSeconds),
		slog.Uint64("bufferPoolSize", uint64(settings.Config.BufferRows)),
		slog.Int("flushPoolSizeKb", settings.Config.FlushSizeKb),
	)

	ctx := context.Background()
	metricsClient := metrics.LoadExporter(settings.Config)
	var dest destination.Baseline
	if utils.IsOutputBaseline(settings.Config) {
		dest, err = utils.LoadBaseline(ctx, settings.Config)
		if err != nil {
			logger.Fatal("Unable to load baseline destination", slog.Any("err", err))
		}
	} else {
		_dest, err := utils.LoadDestination(ctx, settings.Config, nil)
		if err != nil {
			logger.Fatal("Unable to load destination", slog.Any("err", err))
		}

		if err = _dest.SweepTemporaryTables(ctx); err != nil {
			logger.Fatal("Failed to clean up temporary tables", slog.Any("err", err))
		}

		dest = _dest
	}

	slog.Info("Starting...", slog.String("version", version))

	inMemDB := models.NewMemoryDB()
	ctx, err = kafkalib.InjectConsumerProvidersIntoContext(ctx, settings.Config.Kafka)
	if err != nil {
		logger.Fatal("Failed to inject consumer providers into context", slog.Any("err", err))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool[kafka.Message](ctx, inMemDB, dest, metricsClient, settings.Config.Kafka.Topics(), time.Duration(settings.Config.FlushIntervalSeconds)*time.Second)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartKafkaGoConsumer(ctx, settings.Config, inMemDB, dest, metricsClient)
		default:
			logger.Fatal(fmt.Sprintf("Message queue: %q not supported", settings.Config.Queue))
		}
	}(ctx)

	wg.Wait()
}
