package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/artie/metrics"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kinesislib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
)

func StartKinesisConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, whClient *webhooks.Client) {
	encryptionKey, err := cfg.SharedDestinationSettings.BuildEncryptionKey(ctx)
	if err != nil {
		whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
			Error: fmt.Sprintf("Failed to build encryption key: %s", err),
		})
		logger.Fatal("Failed to build encryption key", slog.Any("err", err))
	}

	tcFmtMap := NewTcFmtMap()
	for _, topicConfig := range cfg.Kinesis.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, NewTopicConfigFormatter(*topicConfig, format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic)))
	}

	// Initialize Kinesis client
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Kinesis.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Kinesis.Region))
	}
	if cfg.Kinesis.AwsAccessKeyID != "" && cfg.Kinesis.AwsSecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.Kinesis.AwsAccessKeyID, cfg.Kinesis.AwsSecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
			Error: fmt.Sprintf("Failed to load AWS config for Kinesis: %s", err),
		})
		logger.Fatal("Failed to load AWS config for Kinesis", slog.Any("err", err))
	}

	kinesisClient := kinesis.NewFromConfig(awsCfg)
	consumer, err := kinesislib.NewConsumer(ctx, kinesisClient, cfg.Kinesis.StreamName)
	if err != nil {
		whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
			Error: fmt.Sprintf("Failed to create Kinesis consumer: %s", err),
		})
		logger.Fatal("Failed to create Kinesis consumer", slog.Any("err", err))
	}

	// Used to track offset to skip duplicate processing locally if needed
	// (Kafka does this inside FetchMessageAndProcess)
	partitionToAppliedOffset := make(map[int]int64)

	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if db.IsRetryableError(err, context.DeadlineExceeded) {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to fetch message from Kinesis: %s", err),
			})
			logger.Fatal("Failed to fetch message", slog.Any("err", err))
		}

		if appliedOffset, ok := partitionToAppliedOffset[msg.Partition()]; ok {
			if appliedOffset >= msg.Offset() {
				// We should skip this message because we have already processed it.
				continue
			}
		}

		if len(msg.Value()) == 0 {
			slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
			partitionToAppliedOffset[msg.Partition()] = msg.Offset()
			continue
		}

		args := processArgs{
			Msg:                    msg,
			GroupID:                cfg.Kinesis.StreamName,
			TopicToConfigFormatMap: tcFmtMap,
			WhClient:               whClient,
			EncryptionKey:          encryptionKey,
		}

		tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
		if err != nil {
			whClient.SendEvent(ctx, webhooks.EventReplicationFailed, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to process message: %s", err),
				Topic: msg.Topic(),
			})
			logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", msg.Topic()))
		}

		partitionToAppliedOffset[msg.Partition()] = msg.Offset()

		metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, cfg.Kinesis.StreamName, tableID.Schema, tableID.Table)
		metrics.EmitRowLag(msg, metricsClient, cfg.Mode, cfg.Kinesis.StreamName, tableID.Schema, tableID.Table)
	}
}
