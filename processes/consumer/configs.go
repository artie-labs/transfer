package consumer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type TcFmtMap struct {
	tc map[string]TopicConfigFormatter
	sync.Mutex
}

func NewTcFmtMap() *TcFmtMap {
	return &TcFmtMap{
		tc: make(map[string]TopicConfigFormatter),
	}
}

func (t *TcFmtMap) Add(topic string, fmt TopicConfigFormatter) {
	t.Lock()
	defer t.Unlock()
	t.tc[topic] = fmt
}

func (t *TcFmtMap) GetTopicFmt(topic string) (TopicConfigFormatter, bool) {
	t.Lock()
	defer t.Unlock()
	tcFmt, isOk := t.tc[topic]
	return tcFmt, isOk
}

type TopicConfigFormatter struct {
	tc kafkalib.TopicConfig
	cdc.Format
}

func commitOffset(ctx context.Context, topic string, partitionsToOffset map[string]artie.Message) error {
	for _, msg := range partitionsToOffset {
		if msg.KafkaMsg != nil {
			if err := topicToConsumer.Get(topic).CommitMessages(ctx, *msg.KafkaMsg); err != nil {
				return err
			}

			slog.Info("Successfully committed Kafka offset", slog.String("topic", topic), slog.Int("partition", msg.KafkaMsg.Partition), slog.Int64("offset", msg.KafkaMsg.Offset))
		}
	}

	return nil
}
