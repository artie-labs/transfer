package consumer

import (
	"sync"

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
