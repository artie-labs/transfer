package consumer

import (
	"strings"
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
	tc                kafkalib.TopicConfig
	skipOperationsMap map[string]bool
	cdc.Format
}

func (t TopicConfigFormatter) ShouldSkip(op string) bool {
	if t.skipOperationsMap == nil {
		panic("skipOperationsMap is nil, NewTopicConfigFormatter() was never called")
	}

	_, ok := t.skipOperationsMap[op]
	return ok
}

func NewTopicConfigFormatter(tc kafkalib.TopicConfig, format cdc.Format) TopicConfigFormatter {
	formatter := TopicConfigFormatter{
		tc:                tc,
		skipOperationsMap: make(map[string]bool),
		Format:            format,
	}

	for _, op := range strings.Split(tc.SkippedOperations, ",") {
		formatter.skipOperationsMap[strings.ToLower(strings.TrimSpace(op))] = true
	}

	return formatter
}
