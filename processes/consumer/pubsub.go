package consumer

import (
	gcp_pubsub "cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"
	"google.golang.org/api/option"
	"sync"
	"time"
)

const defaultAckDeadline = 5 * time.Minute

func findOrCreateSubscription(ctx context.Context, client *gcp_pubsub.Client, topic, subID string) (*gcp_pubsub.Subscription, error) {
	log := logger.FromContext(ctx)
	sub := client.Subscription(subID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch subscription, err: %v", err)
	}

	if !exists {
		log.WithField("subID", subID).Info("subscription does not exist, creating one...")
		gcpTopic := client.Topic(topic)
		exists, err = gcpTopic.Exists(ctx)
		if !exists || err != nil {
			// We error out if the topic does not exist or there's an error.
			return nil, fmt.Errorf("failed to fetch gcp topic, exists: %v, err: %v", exists, err)
		}

		sub, err = client.CreateSubscription(ctx, subID, gcp_pubsub.SubscriptionConfig{
			Topic:       gcpTopic,
			AckDeadline: defaultAckDeadline,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to create subscription, subID: %s, err: %v", subID, err)
		}
	}

	return sub, err
}

func StartSubscriber(ctx context.Context, flushChan chan bool) {
	log := logger.FromContext(ctx)
	client, clientErr := gcp_pubsub.NewClient(ctx, config.GetSettings().Config.Pubsub.ProjectID,
		option.WithCredentialsFile(config.GetSettings().Config.Pubsub.PathToCredentials))
	if clientErr != nil {
		log.Fatalf("failed to create a pubsub client, err: %v", clientErr)
	}

	topicToConfigFmtMap := make(map[string]TopicConfigFormatter)
	var topics []string
	for _, topicConfig := range config.GetSettings().Config.Pubsub.TopicConfigs {
		topicToConfigFmtMap[topicConfig.Topic] = TopicConfigFormatter{
			tc:     topicConfig,
			Format: format.GetFormatParser(ctx, topicConfig.CDCFormat),
		}
		topics = append(topics, topicConfig.Topic)
	}

	var wg sync.WaitGroup
	for _, topicConfig := range config.GetSettings().Config.Pubsub.TopicConfigs {
		wg.Add(1)
		go func(ctx context.Context, client *gcp_pubsub.Client, topic string) {
			defer wg.Done()
			subID := fmt.Sprintf("transfer_%s", topic)
			sub, err := findOrCreateSubscription(ctx, client, topic, subID)
			if err != nil {
				log.Fatalf("failed to find or create subscription, err: %v", err)
			}

			err = sub.Receive(ctx, func(_ context.Context, pubsubMsg *gcp_pubsub.Message) {
				msg := artie.NewMessage(nil, pubsubMsg, topic)
				msg.EmitIngestionLag(ctx, subID)
				logFields := map[string]interface{}{
					"topic": msg.Topic(),
					"msgID": msg.PubSub.ID,
					"key":   string(msg.Key()),
					"value": string(msg.Value()),
				}

				shouldFlush, processErr := processMessage(ctx, msg, topicToConfigFmtMap, subID)
				if processErr != nil {
					log.WithError(processErr).WithFields(logFields).Warn("skipping message...")
				}

				if shouldFlush {
					flushChan <- true
				}
			})

			if err != nil {
				log.Fatalf("sub receive error, err: %v", err)
			}
		}(ctx, client, topicConfig.Topic)

	}

	wg.Wait()
}
