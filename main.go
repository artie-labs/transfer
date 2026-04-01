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
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
	"github.com/artie-labs/transfer/processes/pool"
)

var version = "dev" // this will be set by the goreleaser configuration to appropriate value for the compiled binary.

func main() {
	// Parse args into settings
	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	var webhookSettings *config.WebhookSettings
	if settings != nil {
		webhookSettings = settings.Config.WebhookSettings
	}
	whClient, whErr := webhooks.NewClient(webhookSettings, webhooks.Transfer, version)
	if whErr != nil {
		logger.Fatal("Failed to initialize webhooks client", slog.Any("err", whErr))
	}

	if err != nil {
		if whClient != nil {
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to initialize: %s", err),
			})
		}
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
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to parse max init sleep duration: %s", err),
			})
			logger.Fatal("Failed to parse max init sleep duration", slog.Any("err", err), slog.String("value", value))
		}

		randomSeconds, err := cryptography.RandomInt64n(castedValue)
		if err != nil {
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to generate sleep duration: %s", err),
			})
			logger.Fatal("Failed to generate sleep duration", slog.Any("err", err))
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

	metricsClient := metrics.LoadExporter(settings.Config)
	dest, err := utils.Load(ctx, settings.Config)
	if err != nil {
		whClient.SendEvent(ctx, webhooks.EventConnectionFailed, webhooks.EventProperties{
			Error: fmt.Sprintf("Unable to load destination: %s", err),
		})
		logger.Fatal("Unable to load destination", slog.Any("err", err))
	}

	if sqlDest, ok := dest.(destination.SQLDestination); ok {
		if err = sqlDest.SweepTemporaryTables(ctx); err != nil {
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to clean up temporary tables: %s", err),
			})
			logger.Fatal("Failed to clean up temporary tables", slog.Any("err", err))
		}
	}

	slog.Info("Starting...", slog.String("version", version))
	whClient.SendEvent(ctx, webhooks.EventReplicationStarted, webhooks.EventProperties{})

	inMemDB := models.NewMemoryDB()
	switch settings.Config.KafkaClient {
	case config.FranzGoClient:
		ctx, err = kafkalib.InjectFranzGoConsumerProvidersIntoContext(ctx, settings.Config.Kafka)
		if err != nil {
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to initialize Kafka client: %s", err),
			})
			logger.Fatal("Failed to inject franz-go consumer providers into context", slog.Any("err", err))
		}
	default:
		whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
			Error: fmt.Sprintf("Failed to initialize: Kafka client %q not supported", settings.Config.KafkaClient),
		})
		logger.Fatal(fmt.Sprintf("Kafka client: %q not supported", settings.Config.KafkaClient))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer logger.RecoverFatal()
		pool.StartPool(ctx, inMemDB, dest, metricsClient, whClient, settings.Config.Kafka.Topics(), time.Duration(settings.Config.FlushIntervalSeconds)*time.Second, settings.Config)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		defer logger.RecoverFatal()
		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartKafkaConsumer(ctx, settings.Config, inMemDB, dest, metricsClient, whClient)
		case constants.Kinesis:
			consumer.StartKinesisConsumer(ctx, settings.Config, inMemDB, dest, metricsClient, whClient)
		default:
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to initialize: message queue %q not supported", settings.Config.Queue),
			})
			logger.Fatal(fmt.Sprintf("Message queue: %q not supported", settings.Config.Queue))
		}
	}(ctx)

	wg.Wait()
}
