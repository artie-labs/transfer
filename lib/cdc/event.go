package cdc

import (
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Format interface {
	Labels() []string // Labels() to return a list of strings to maintain backward compatibility.
	GetPrimaryKey(key []byte, tc kafkalib.TopicConfig) (map[string]any, error)
	GetEventFromBytes(bytes []byte) (Event, error)
}

type Event interface {
	GetExecutionTime() time.Time
	Operation() string
	DeletePayload() bool
	GetTableName() string
	GetSourceMetadata() (string, error)
	GetData(tc kafkalib.TopicConfig) (map[string]any, error)
	GetOptionalSchema() (map[string]typing.KindDetails, error)
	// GetColumns will inspect the envelope's payload right now and return.
	GetColumns() (*columns.Columns, error)
}
