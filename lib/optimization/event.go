package optimization

import (
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
)

type TableData struct {
	Columns    map[string]typing.Kind            // list of columns
	RowsData   map[string]map[string]interface{} // pk -> { col -> val }
	PrimaryKey string

	kafkalib.TopicConfig
	// Partition to the latest offset.
	PartitionsToOffset map[int32]string

	// This is used for the automatic schema detection
	LatestCDCTs time.Time

	Rows uint
}
