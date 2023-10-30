package consumer

import (
	"context"
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
	return
}

func (t *TcFmtMap) GetTopicFmt(topic string) (TopicConfigFormatter, bool) {
	t.Lock()
	defer t.Unlock()
	tcFmt, isOk := t.tc[topic]
	return tcFmt, isOk
}

type TopicConfigFormatter struct {
	tc *kafkalib.TopicConfig
	cdc.Format
}

func commitOffset(ctx context.Context, topic string, partitionsToOffset map[string][]artie.Message) error {
	var err error
	for _, msgs := range partitionsToOffset {
		for _, msg := range msgs {
			if msg.KafkaMsg != nil {
				err = topicToConsumer.Get(topic).CommitMessages(ctx, *msg.KafkaMsg)
				if err != nil {
					return err
				}
			}

			if msg.PubSub != nil {
				msg.PubSub.Ack()
			}
		}
	}

	return err
}
