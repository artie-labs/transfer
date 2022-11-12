package cdc

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Format interface {
	Label() string
	GetPrimaryKey(ctx context.Context, key []byte) (string, interface{}, error)
	GetEventFromBytes(ctx context.Context, bytes []byte) (Event, error)
}

type Event interface {
	GetExecutionTime() time.Time
	Table() string
	GetData(pkName string, pkVal interface{}, config kafkalib.TopicConfig) map[string]interface{}
}
