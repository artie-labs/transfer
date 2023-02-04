package optimization

import (
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

type TableData struct {
	InMemoryColumns map[string]typing.Kind            // list of columns
	RowsData        map[string]map[string]interface{} // pk -> { col -> val }
	PrimaryKey      string

	kafkalib.TopicConfig
	// Partition to the latest offset.
	PartitionsToLastMessage map[int]kafka.Message

	// This is used for the automatic schema detection
	LatestCDCTs time.Time

	Rows uint
}
