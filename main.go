package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
	"github.com/artie-labs/transfer/lib/webhooksutil"
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
	whClient, whErr := webhooksclient.NewFromConfig(webhookSettings)
	if whErr != nil {
		logger.Fatal("Failed to initialize webhooks client", slog.Any("err", whErr))
	}

	if err != nil {
		whClient.SendEvent(ctx, webhooksutil.ConfigInvalid, map[string]any{
			"error":   "Failed to initialize config",
			"details": err.Error(),
		})
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

	metricsClient := metrics.LoadExporter(settings.Config)
	dest, err := utils.Load(ctx, settings.Config)
	if err != nil {
		whClient.SendEvent(ctx, webhooksutil.ConnectionFailed, map[string]any{
			"error":   "Unable to load destination",
			"details": err.Error(),
		})
		logger.Fatal("Unable to load destination", slog.Any("err", err))
	}

	if livyDest, ok := dest.(interface{ GetApacheLivyClient() *apachelivy.Client }); ok {
		livyDest.GetApacheLivyClient().SetMetricsClient(metricsClient)
	}

	if sqlDest, ok := dest.(destination.SQLDestination); ok {
		if err = sqlDest.SweepTemporaryTables(ctx, whClient); err != nil {
			whClient.SendEvent(ctx, webhooksutil.ConnectionFailed, map[string]any{
				"error":   "Failed to clean up temporary tables",
				"details": err.Error(),
			})
			logger.Fatal("Failed to clean up temporary tables", slog.Any("err", err))
		}

		whClient.SendEvent(ctx, webhooksutil.ConnectionEstablished, map[string]any{
			"mode": settings.Config.Mode,
		})
	}

	slog.Info("Starting...", slog.String("version", version))
	whClient.SendEvent(ctx, webhooksutil.ReplicationStarted, map[string]any{
		"version": version,
		"mode":    settings.Config.Mode,
	})

	inMemDB := models.NewMemoryDB()
	switch settings.Config.KafkaClient {
	case config.FranzGoClient:
		ctx, err = kafkalib.InjectFranzGoConsumerProvidersIntoContext(ctx, settings.Config.Kafka)
		if err != nil {
			logger.Fatal("Failed to inject franz-go consumer providers into context", slog.Any("err", err))
		}
	default:
		logger.Fatal(fmt.Sprintf("Kafka client: %q not supported", settings.Config.KafkaClient))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.StartPool(ctx, inMemDB, dest, metricsClient, whClient, settings.Config.Kafka.Topics(), time.Duration(settings.Config.FlushIntervalSeconds)*time.Second)
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		switch settings.Config.Queue {
		case constants.Kafka:
			consumer.StartKafkaConsumer(ctx, settings.Config, inMemDB, dest, metricsClient, whClient)
		default:
			logger.Fatal(fmt.Sprintf("Message queue: %q not supported", settings.Config.Queue))
		}
	}(ctx)

	wg.Wait()
}
