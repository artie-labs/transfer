package optimization

import (
	"github.com/artie-labs/transfer/lib/artie"
	"strings"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type TableData struct {
	inMemoryColumns map[string]typing.KindDetails     // list of columns
	rowsData        map[string]map[string]interface{} // pk -> { col -> val }
	primaryKeys     []string

	topicConfig kafkalib.TopicConfig
	// Partition to the latest offset(s).
	// For Kafka, we only need the last message to commit the offset
	// However, pub/sub requires every single message to be acked
	partitionsToLastMessage map[string][]artie.Message

	// This is used for the automatic schema detection
	latestCDCTs time.Time

	// Mutex should be at the table level.
	sync.Mutex
}

// UpdatePartitionsToLastMessage is used to commit the offset when flush is successful
// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
func (t *TableData) UpdatePartitionsToLastMessage(message artie.Message, cdcTs time.Time) {
	t.Lock()
	defer t.Unlock()

	if message.Kind() == artie.Kafka {
		t.partitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		t.partitionsToLastMessage[message.Partition()] = append(t.partitionsToLastMessage[message.Partition()], message)
	}

	// This is a metadata used for column detection.
	t.latestCDCTs = cdcTs
}

func (t *TableData) AddRowData(primaryKey string, rowData map[string]interface{}) {
	t.Lock()
	defer t.Unlock()

	t.rowsData[primaryKey] = rowData
}

func (t *TableData) InMemoryColumns() map[string]typing.KindDetails {
	t.Lock()
	defer t.Unlock()
	return t.inMemoryColumns
}

func (t *TableData) Rows() uint {
	// TODO: Is this concurrent safe?
	if t == nil {
		return 0
	}

	return uint(len(t.rowsData))
}

func (t *TableData) ModifyColumnType(name string, kindDetails typing.KindDetails) {
	t.Lock()
	defer t.Unlock()
	t.inMemoryColumns[name] = kindDetails
}

// UpdateInMemoryColumns - When running Transfer, we will have 2 column types.
// TODO: This needs to be more clear
// 1) TableData (constructed in-memory)
// 2) TableConfig (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfig. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) UpdateInMemoryColumns(cols map[string]typing.KindDetails) {
	if t == nil {
		return
	}

	t.Lock()
	defer t.Unlock()

	for inMemCol, inMemKindDetails := range t.inMemoryColumns {
		if inMemKindDetails.Kind == typing.Invalid.Kind {
			// Don't copy this over.
			// The being that the rows within tableData probably have the wrong colVal
			// So it's better to skip even attempting to create this column from memory values.
			// Whenever we get the first value that's a not-nil or invalid, this column type will be updated.
			continue
		}

		// strings.ToLower() is used because certain destinations do not follow JSON standards.
		// Snowflake and BigQuery consider: NaMe, NAME, name as the same value. Whereas JSON considers these as 3 distinct values.
		tcKind, isOk := cols[strings.ToLower(inMemCol)]
		if isOk {
			// Update in-memory column type with whatever is specified by the destination
			inMemKindDetails.Kind = tcKind.Kind
			if tcKind.ExtendedTimeDetails != nil {
				if inMemKindDetails.ExtendedTimeDetails == nil {
					inMemKindDetails.ExtendedTimeDetails = &ext.NestedKind{}
				}

				// Don't have tcKind.ExtendedTimeDetails update the format since the DWH will not have that.
				inMemKindDetails.ExtendedTimeDetails.Type = tcKind.ExtendedTimeDetails.Type
			}

			t.inMemoryColumns[inMemCol] = inMemKindDetails
		}
	}

	return
}
