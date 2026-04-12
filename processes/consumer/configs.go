package consumer

import (
	"strings"
	"sync"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type TcFmtMap struct {
	tc map[string]TopicConfigFormatter
	sync.RWMutex
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
	t.RLock()
	defer t.RUnlock()
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

// buildPKMap returns the primary key map for an event. When the Kafka message key is empty and primary keys are
// specified via [TopicConfig.PrimaryKeysOverride] or [TopicConfig.IncludePrimaryKeys], key parsing is skipped
// and an empty map is returned -- the PK column names come from config and values come from the event payload.
func (t TopicConfigFormatter) buildPKMap(key []byte, reservedColumns map[string]bool) (map[string]any, error) {
	if len(key) == 0 && (len(t.tc.PrimaryKeysOverride) > 0 || len(t.tc.IncludePrimaryKeys) > 0) {
		return map[string]any{}, nil
	}

	return t.GetPrimaryKey(key, t.tc, reservedColumns)
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
