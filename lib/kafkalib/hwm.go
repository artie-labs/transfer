package kafkalib

import (
	"context"
	"fmt"
	"sync"
)

type ContextKey string

const ctxKey ContextKey = "__hwm"

type HighWaterMark struct {
	// [topicToPartitionHWM] - This is used to track the high watermark for each partition in a topic.
	topicToPartitionHWM map[string]map[int]int64
	sync.RWMutex
}

func (h *HighWaterMark) GetHWM(topic string, partition int) (int64, bool) {
	h.Lock()
	defer h.Unlock()
	hwm, ok := h.topicToPartitionHWM[topic][partition]
	return hwm, ok
}

func (h *HighWaterMark) SetHWM(topic string, partition int, hwm int64) {
	h.Lock()
	defer h.Unlock()
	h.topicToPartitionHWM[topic][partition] = hwm
}

func GetHWMFromContext(ctx context.Context) (*HighWaterMark, error) {
	hwm, ok := ctx.Value(ctxKey).(*HighWaterMark)
	if !ok {
		return nil, fmt.Errorf("hwm not found in context")
	}

	return hwm, nil
}

func InjectHWMIntoContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey, &HighWaterMark{
		topicToPartitionHWM: map[string]map[int]int64{},
	})
}
