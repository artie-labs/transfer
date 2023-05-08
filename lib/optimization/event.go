package optimization

import (
	"context"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"strings"
	"time"
)

type TableData struct {
	InMemoryColumns *typing.Columns                   // list of columns
	rowsData        map[string]map[string]interface{} // pk -> { col -> val }
	PrimaryKeys     []string

	kafkalib.TopicConfig
	// Partition to the latest offset(s).
	// For Kafka, we only need the last message to commit the offset
	// However, pub/sub requires every single message to be acked
	PartitionsToLastMessage map[string][]artie.Message

	// This is used for the automatic schema detection
	LatestCDCTs time.Time

	approxSize int
}

func NewTableData(inMemoryColumns *typing.Columns, primaryKeys []string, topicConfig kafkalib.TopicConfig) *TableData {
	return &TableData{
		InMemoryColumns:         inMemoryColumns,
		rowsData:                map[string]map[string]interface{}{},
		PrimaryKeys:             primaryKeys,
		TopicConfig:             topicConfig,
		PartitionsToLastMessage: map[string][]artie.Message{},
	}

}

func (t *TableData) InsertRow(pk string, rowData map[string]interface{}) {
	// TODO: Test.
	newRowSize := size.GetApproxSize(rowData)
	prevRow, isOk := t.rowsData[pk]
	if isOk {
		// Since the new row is taking over, let's update the approx size.
		prevSize := size.GetApproxSize(prevRow)
		t.approxSize += newRowSize - prevSize
	} else {
		t.approxSize += newRowSize
	}

	t.rowsData[pk] = rowData
	return
}

// RowsData returns a read only map of tableData's rowData.
func (t *TableData) RowsData() map[string]map[string]interface{} {
	_rowsData := make(map[string]map[string]interface{}, len(t.rowsData))
	for k, v := range t.rowsData {
		_rowsData[k] = v
	}

	return _rowsData
}

func (t *TableData) Rows() uint {
	if t == nil {
		return 0
	}

	return uint(len(t.rowsData))
}

func (t *TableData) ShouldFlush(ctx context.Context) bool {
	// TODO Test
	settings := config.FromContext(ctx)

	return t.Rows() > settings.Config.BufferRows || t.approxSize > settings.Config.FlushSizeKb * 1024
}

// UpdateInMemoryColumnsFromDestination - When running Transfer, we will have 2 column types.
// 1) TableData (constructed in-memory)
// 2) TableConfig (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfig. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) UpdateInMemoryColumnsFromDestination(cols ...typing.Column) {
	if t == nil {
		return
	}

	for _, inMemoryCol := range t.InMemoryColumns.GetColumns() {
		if inMemoryCol.KindDetails.Kind == typing.Invalid.Kind {
			// Don't copy this over because tableData has the wrong colVal
			continue
		}

		var foundColumn typing.Column
		var found bool
		for _, col := range cols {
			if col.Name == strings.ToLower(inMemoryCol.Name) {
				foundColumn = col
				found = true
				break
			}
		}

		if found {
			inMemoryCol.KindDetails.Kind = foundColumn.KindDetails.Kind
			if foundColumn.KindDetails.ExtendedTimeDetails != nil {
				if inMemoryCol.KindDetails.ExtendedTimeDetails == nil {
					inMemoryCol.KindDetails.ExtendedTimeDetails = &ext.NestedKind{}
				}

				// Don't have tcKind.ExtendedTimeDetails update the format since the DWH will not have that.
				inMemoryCol.KindDetails.ExtendedTimeDetails.Type = foundColumn.KindDetails.ExtendedTimeDetails.Type
			}

			t.InMemoryColumns.UpdateColumn(inMemoryCol)
		}
	}

	return
}
