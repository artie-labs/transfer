package cdc

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Format interface {
	Label() string
	GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (string, interface{}, error)
	GetEventFromBytes(ctx context.Context, bytes []byte) (Event, error)
}

type Event interface {
	Table() string
	GetExecutionTime() time.Time
	GetData(pkName string, pkVal interface{}, config *kafkalib.TopicConfig) map[string]interface{}
}

// FieldLabelKind is used when the schema is turned on. Each schema object will be labelled.
type FieldLabelKind string

const (
	Before      FieldLabelKind = "before"
	After       FieldLabelKind = "after"
	Source      FieldLabelKind = "source"
	Op          FieldLabelKind = "op"
	TsMs        FieldLabelKind = "ts_ms"
	Transaction FieldLabelKind = "transaction"
)
