package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

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
	// Create a context with timeout for commits to ensure they don't hang
	commitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for _, msg := range partitionsToOffset {
		slog.Info("Attempting to commit offset",
			slog.String("topic", topic),
			slog.Int("partition", msg.Partition()),
			slog.Int64("offset", msg.GetMessage().Offset))

		if err := topicToConsumer.Get(topic).CommitMessages(commitCtx, msg.GetMessage()); err != nil {
			slog.Error("Commit failed",
				slog.String("topic", topic),
				slog.Int("partition", msg.Partition()),
				slog.Int64("offset", msg.GetMessage().Offset),
				slog.Any("err", err))
			return err
		}

		slog.Info("Successfully committed Kafka offset", slog.String("topic", topic), slog.Int("partition", msg.Partition()), slog.Int64("offset", msg.GetMessage().Offset))
	}

	return nil
}
