package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gcp_pubsub "cloud.google.com/go/pubsub"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"
	"google.golang.org/api/option"
)

const defaultAckDeadline = 10 * time.Minute

func findOrCreateSubscription(ctx context.Context, client *gcp_pubsub.Client, topic, subName string) (*gcp_pubsub.Subscription, error) {
	sub := client.Subscription(subName)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch subscription, err: %v", err)
	}

	if !exists {
		slog.Info("subscription does not exist, creating one...", slog.String("topic", topic))
		gcpTopic := client.Topic(topic)
		exists, err = gcpTopic.Exists(ctx)
		if !exists || err != nil {
			// We error out if the topic does not exist or there's an error.
			return nil, fmt.Errorf("failed to fetch gcp topic, topic exists: %v, err: %v", exists, err)
		}

		sub, err = client.CreateSubscription(ctx, subName, gcp_pubsub.SubscriptionConfig{
			Topic:       gcpTopic,
			AckDeadline: defaultAckDeadline,
			// Enable ordering given the `partition key` which is known as ordering key in Pub/Sub
			EnableMessageOrdering: true,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to create subscription, topic: %s, err: %v", topic, err)
		}
	}

	// This should be the same as our buffer rows so we don't limit our processing throughput
	sub.ReceiveSettings.MaxOutstandingMessages = int(config.FromContext(ctx).Config.BufferRows) + 1

	// By default, the pub/sub library will try to spawns 10 additional Go-routines per subscription,
	// it actually does not make the process faster. Rather, it creates more coordination overhead.
	// Our process message is already extremely fast (~100-200 ns), so we're reducing this down to 1.
	sub.ReceiveSettings.NumGoroutines = 1
	return sub, err
}

func StartSubscriber(ctx context.Context) {
	settings := config.FromContext(ctx)
	client, clientErr := gcp_pubsub.NewClient(ctx, settings.Config.Pubsub.ProjectID,
		option.WithCredentialsFile(settings.Config.Pubsub.PathToCredentials))
	if clientErr != nil {
		logger.Panic("Failed to create a pubsub client", slog.Any("err", clientErr))
	}

	tcFmtMap := NewTcFmtMap()
	for _, topicConfig := range settings.Config.Pubsub.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, TopicConfigFormatter{
			tc:     topicConfig,
			Format: format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic),
		})
	}

	var wg sync.WaitGroup
	for _, topicConfig := range settings.Config.Pubsub.TopicConfigs {
		wg.Add(1)
		go func(ctx context.Context, client *gcp_pubsub.Client, topic string) {
			defer wg.Done()
			subName := fmt.Sprintf("transfer_%s", topic)
			sub, err := findOrCreateSubscription(ctx, client, topic, subName)
			if err != nil {
				logger.Panic("Failed to find or create subscription", slog.Any("err", err))
			}

			for {
				err = sub.Receive(ctx, func(_ context.Context, pubsubMsg *gcp_pubsub.Message) {
					msg := artie.NewMessage(nil, pubsubMsg, topic)
					logFields := []any{
						slog.String("topic", msg.Topic()),
						slog.String("msgID", msg.PubSub.ID),
						slog.String("key", string(msg.Key())),
						slog.String("value", string(msg.Value())),
					}

					tableName, processErr := processMessage(ctx, ProcessArgs{
						Msg:                    msg,
						GroupID:                subName,
						TopicToConfigFormatMap: tcFmtMap,
					})

					msg.EmitIngestionLag(ctx, subName, tableName)
					if processErr != nil {
						slog.With(logFields...).Warn("skipping message...", slog.Any("err", processErr))
					}
				})

				if err != nil {
					logger.Panic("Sub receive error", slog.Any("err", err))
				}
			}

		}(ctx, client, topicConfig.Topic)

	}

	wg.Wait()
}
