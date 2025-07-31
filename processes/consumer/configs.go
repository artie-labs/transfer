package consumer

import (
	"context"
	"fmt"
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
	tcFmt, ok := t.tc[topic]
	return tcFmt, ok
}

type TopicConfigFormatter struct {
	tc kafkalib.TopicConfig
	cdc.Format
}

func commitOffset(ctx context.Context, topic string, partitionsToOffset map[int]artie.Message) error {
	for _, msg := range partitionsToOffset {
		if err := topicToConsumer.Get(topic).CommitMessages(ctx, msg.GetMessage()); err != nil {
			return err
		}

		slog.Info("Successfully committed Kafka offset", slog.String("topic", topic), slog.Int("partition", msg.Partition()), slog.Int64("offset", msg.GetMessage().Offset))
	}

	hwm, err := kafkalib.GetHWMFromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get HWM from context: %w", err)
	}

	for _, msg := range partitionsToOffset {
		hwm.SetHWM(topic, msg.Partition(), msg.GetMessage().Offset)
	}

	return nil
}
