package cdc

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
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
	Operation() constants.Operation
	DeletePayload() bool
	GetTableName() string
	GetFullTableName() string
	GetSourceMetadata() (string, error)
	GetData(tc kafkalib.TopicConfig) (map[string]any, error)
	GetOptionalSchema() (map[string]typing.KindDetails, error)
	// GetColumns will inspect the envelope's payload right now and return.
	GetColumns() (*columns.Columns, error)
}

type TableID struct {
	Schema string
	Table  string
}

func NewTableID(schema, table string) TableID {
	return TableID{
		Schema: schema,
		Table:  table,
	}
}

func (t TableID) IsEmpty() bool {
	return t.Schema == "" && t.Table == ""
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}
