package optimization

import (
	"github.com/artie-labs/transfer/lib/artie"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type TableData struct {
	InMemoryColumns map[string]typing.KindDetails     // list of columns
	RowsData        map[string]map[string]interface{} // pk -> { col -> val }
	PrimaryKey      string

	kafkalib.TopicConfig
	// Partition to the latest offset.
	PartitionsToLastMessage map[string][]artie.Message

	// This is used for the automatic schema detection
	LatestCDCTs time.Time

	Rows uint
}

// UpdateInMemoryColumns - When running Transfer, we will have 2 column types.
// 1) TableData (constructed in-memory)
// 2) TableConfig (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfig. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) UpdateInMemoryColumns(cols map[string]typing.KindDetails) {
	if t == nil {
		return
	}

	for inMemCol, inMemKindDetails := range t.InMemoryColumns {
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

			t.InMemoryColumns[inMemCol] = inMemKindDetails
		}
	}

	return
}
