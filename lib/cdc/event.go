package cdc

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Format interface {
	Labels() []string // Labels() to return a list of strings to maintain backward compatibility.
	GetPrimaryKey(ctx context.Context, key []byte, tc *kafkalib.TopicConfig) (map[string]interface{}, error)
	GetEventFromBytes(ctx context.Context, bytes []byte) (Event, error)
}

type Event interface {
	GetExecutionTime() time.Time
	Operation() string
	DeletePayload() bool
	GetTableName() string
	GetData(ctx context.Context, pkMap map[string]interface{}, config *kafkalib.TopicConfig) map[string]interface{}
	GetOptionalSchema(ctx context.Context) map[string]typing.KindDetails
	// GetColumns will inspect the envelope's payload right now and return.
	GetColumns(ctx context.Context) *columns.Columns
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
