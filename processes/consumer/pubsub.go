package consumer

import (
	gcp_pubsub "cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"sync"
	"time"
)

func findOrCreateSubscription(ctx context.Context, client *gcp_pubsub.Client, topic, subID string) (*gcp_pubsub.Subscription, error) {
	log := logger.FromContext(ctx)
	// Check if subscription exists
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
			AckDeadline: constants.FlushTimeInterval * 2,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to create subscription, err: %v", err)
		}
	}

	return sub, err
}

func StartSubscriber(ctx context.Context, flushChan chan bool) {
	// TODO: Publish documentation regarding PubSub on our docs.
	log := logger.FromContext(ctx)

	// TODO need to inject credentials into ENV_VAR
	// TODO - Is this going to run into a problem with BQ credentials if they are different?
	client, err := gcp_pubsub.NewClient(ctx, config.GetSettings().Config.Pubsub.ProjectID)
	if err != nil {
		log.Fatalf("failed to create a pubsub client, err: %v", err)
	}

	var wg sync.WaitGroup
	for _, topicConfig := range config.GetSettings().Config.Pubsub.TopicConfigs {
		wg.Add(1)
		go func(ctx context.Context, client *gcp_pubsub.Client, topic string) {
			defer wg.Done()
			subID := fmt.Sprintf("transfer_%s", config.GetSettings().Config.Pubsub)
			sub, err := findOrCreateSubscription(ctx, client, topic, subID)
			if err != nil {
				log.Fatalf("failed to find or create subscription, err: %v", err)
			}

			err = sub.Receive(ctx, func(_ context.Context, msg *gcp_pubsub.Message) {
				// do stuff
				metrics.FromContext(ctx).Timing("ingestion.lag", time.Since(msg.PublishTime), map[string]string{
					"topic":        topic,
					"subscription": subID,
				})

			})

			if err != nil {
				log.Fatalf("sub receive error, err: %v", err)
			}
		}(ctx, client, topicConfig.Topic)

	}

	wg.Wait()
}
