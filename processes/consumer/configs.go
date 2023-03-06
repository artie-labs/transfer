package consumer

import (
	"context"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type TopicConfigFormatter struct {
	tc *kafkalib.TopicConfig
	cdc.Format
}

func CommitOffset(ctx context.Context, topic string, partitionsToOffset map[string]artie.Message) error {
	var err error
	for _, msg := range partitionsToOffset {
		if msg.KafkaMsg != nil {
			err = topicToConsumer[topic].CommitMessages(ctx, *msg.KafkaMsg)
			if err != nil {
				return err
			}
		}

		if msg.PubSub != nil {
			msg.PubSub.Ack()
		}
	}

	return err
}
