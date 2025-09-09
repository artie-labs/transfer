package optimization

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (m MultiStepMergeSettings) IsFirstFlush() bool {
	return m.flushCount == 0
}

func (m MultiStepMergeSettings) IsLastFlush() bool {
	return m.flushCount == m.TotalFlushCount
}

func (m MultiStepMergeSettings) FlushCount() int {
	return m.flushCount
}

func (m *MultiStepMergeSettings) Increment() {
	m.flushCount++
}

type MultiStepMergeSettings struct {
	Enabled         bool
	flushCount      int
	TotalFlushCount int
}

type TableData struct {
	mode            config.Mode
	inMemoryColumns *columns.Columns // list of columns

	// rowsData is used for replication
	rowsData map[string]Row // pk -> { col -> val }
	// rows is used for history mode, since it's append only.
	rows []Row

	primaryKeys []string

	topicConfig kafkalib.TopicConfig

	// [latestTimestamp] - This property is used for the automatic schema detection
	latestTimestamp time.Time
	approxSize      int
	// containOtherOperations - this means the `TableData` object contains other events that arises from CREATE, UPDATE, REPLICATION
	// if this value is false, that means it is only deletes. Which means we should not drop columns
	containOtherOperations bool

	// containsHardDeletes - this means there are hard deletes in `rowsData`, so for multi-part merge statements, we should include a DELETE SQL statement.
	containsHardDeletes bool

	temporaryTableSuffix string

	// Multi-step merge settings
	multiStepMergeSettings MultiStepMergeSettings

	// Name of the table in the destination
	name string
}

func (t *TableData) SetLatestTimestamp(timestamp time.Time) {
	t.latestTimestamp = timestamp
}

func (t *TableData) GetLatestTimestamp() time.Time {
	return t.latestTimestamp
}

func (t *TableData) MultiStepMergeSettings() MultiStepMergeSettings {
	return t.multiStepMergeSettings
}

func (t *TableData) IncrementMultiStepMergeFlushCount() {
	t.multiStepMergeSettings.Increment()
}

func (t *TableData) WipeData() {
	t.rowsData = make(map[string]Row)
	t.rows = []Row{}
	t.approxSize = 0
	t.ResetTempTableSuffix()
}

func (t *TableData) Mode() config.Mode {
	return t.mode
}

// ShouldSkipUpdate will check if there are any rows or any columns
func (t *TableData) ShouldSkipUpdate() bool {
	return t.NumberOfRows() == 0 || t.ReadOnlyInMemoryCols() == nil
}

func (t *TableData) ContainsHardDeletes() bool {
	return t.containsHardDeletes
}

func (t *TableData) ContainOtherOperations() bool {
	return t.containOtherOperations
}

func (t *TableData) PrimaryKeys() []string {
	return t.primaryKeys
}

func (t *TableData) Name() string {
	return t.name
}

func (t *TableData) InMemoryColumns() *columns.Columns {
	return t.inMemoryColumns
}

func (t *TableData) SetInMemoryColumns(columns *columns.Columns) {
	t.inMemoryColumns = columns
}

func (t *TableData) AddInMemoryCol(column columns.Column) {
	t.inMemoryColumns.AddColumn(column)
}

func (t *TableData) ReadOnlyInMemoryCols() *columns.Columns {
	if t.inMemoryColumns == nil {
		return nil
	}

	var cols columns.Columns
	for _, col := range t.inMemoryColumns.GetColumns() {
		cols.AddColumn(col)
	}

	return &cols
}

func (t *TableData) TopicConfig() kafkalib.TopicConfig {
	return t.topicConfig
}

func NewTableData(inMemoryColumns *columns.Columns, mode config.Mode, primaryKeys []string, topicConfig kafkalib.TopicConfig, name string) *TableData {
	td := TableData{
		mode:            mode,
		inMemoryColumns: inMemoryColumns,
		rowsData:        map[string]Row{},
		primaryKeys:     primaryKeys,
		topicConfig:     topicConfig,
		// temporaryTableSuffix is being set in `ResetTempTableSuffix`
		temporaryTableSuffix: "",
		name:                 name,
	}

	if multiStepMergeSettings := topicConfig.MultiStepMergeSettings; multiStepMergeSettings != nil {
		td.multiStepMergeSettings = MultiStepMergeSettings{
			Enabled:         multiStepMergeSettings.Enabled,
			TotalFlushCount: multiStepMergeSettings.FlushCount,
		}
	}

	return &td
}

// InsertRow creates a single entrypoint for how rows get added to TableData
// This is important to avoid concurrent r/w, but also the ability for us to add or decrement row size by keeping a running total
// With this, we are able to reduce the latency by 500x+ on a 5k row table. See event_bench_test.go vs. size_bench_test.go
func (t *TableData) InsertRow(pk string, rowData map[string]any, delete bool) {
	newRow := NewRow(rowData)
	if t.mode == config.History {
		t.rows = append(t.rows, newRow)
		t.approxSize += newRow.GetApproxSize()
		return
	}

	var prevRowSize int
	if prevRow, ok := t.rowsData[pk]; ok {
		prevRowSize = prevRow.GetApproxSize()
		if delete {
			prevRowData := prevRow.GetData()
			// If the current row was deleted, we should preserve the previous values that we have in memory
			// However, if the current row has a value for the column, then we should use that value instead.
			for key, value := range prevRowData {
				if rowData[key] == nil {
					rowData[key] = value
				}
			}

			// Setting this to the previous row for idempotency.
			rowData[constants.OnlySetDeleteColumnMarker] = prevRowData[constants.OnlySetDeleteColumnMarker]
		} else {
			for key, val := range rowData {
				if val == constants.ToastUnavailableValuePlaceholder {
					// Copy it from prevRow.
					prevVal, ok := prevRow.GetValue(key)
					if !ok {
						continue
					}

					// If we got back a TOASTED value, we need to use the previous row.
					rowData[key] = prevVal
				}
			}

		}
	}

	newRowSize := size.GetApproxSize(rowData)
	// If prevRow doesn't exist, it'll be 0, which is a no-op.
	t.approxSize += newRowSize - prevRowSize
	t.rowsData[pk] = NewRow(rowData)
	if !delete {
		t.containOtherOperations = true
	} else if delete && !t.topicConfig.SoftDelete {
		// If there's an actual hard delete, let's update it.
		// We know because we have a delete operation and this topic is not configured to do soft deletes.
		t.containsHardDeletes = true
	}
}

func (t *TableData) Rows() []Row {
	if t.Mode() == config.History {
		// History mode, the data is stored under `rows`
		return t.rows
	}

	var rows []Row
	for _, row := range t.rowsData {
		rows = append(rows, row)
	}

	return rows
}

func (t *TableData) NumberOfRows() uint {
	if t == nil {
		return 0
	}

	if t.mode == config.History {
		return uint(len(t.rows))
	}

	return uint(len(t.rowsData))
}

func (t *TableData) ResetTempTableSuffix() {
	if t == nil {
		// This is needed because we periodically wipe tableData
		return
	}

	// Lowercase this because BigQuery is case-sensitive.
	t.temporaryTableSuffix = strings.ToLower(stringutil.Random(5))
}

func (t *TableData) TempTableSuffix() string {
	return t.temporaryTableSuffix
}

// ShouldFlush will return whether Transfer should flush
// If so, what is the reason?
func (t *TableData) ShouldFlush(cfg config.Config) (bool, string) {
	if t.NumberOfRows() > cfg.BufferRows {
		return true, "rows"
	}

	if t.approxSize > cfg.FlushSizeKb*1024 {
		return true, "size"
	}

	return false, ""
}

func (t *TableData) BuildColumnsToKeep() []string {
	colsMap := maputil.NewOrderedMap[bool](false)
	if t.Mode() == config.History {
		colsMap.Add(constants.OperationColumnMarker, true)
	}

	if t.TopicConfig().IncludeArtieOperation {
		colsMap.Add(constants.OperationColumnMarker, true)
	}

	if t.TopicConfig().SoftDelete {
		colsMap.Add(constants.DeleteColumnMarker, true)
	}

	if t.TopicConfig().IncludeArtieUpdatedAt {
		colsMap.Add(constants.UpdateColumnMarker, true)
	}

	if t.TopicConfig().IncludeDatabaseUpdatedAt {
		colsMap.Add(constants.DatabaseUpdatedColumnMarker, true)
	}

	if t.TopicConfig().IncludeSourceMetadata {
		colsMap.Add(constants.SourceMetadataColumnMarker, true)
	}

	if t.TopicConfig().IncludeFullSourceTableName {
		colsMap.Add(constants.FullSourceTableNameColumnMarker, true)
	}

	return colsMap.Keys()
}

// MergeColumnsFromDestination - When running Transfer, we will have 2 column types.
// 1) TableData (constructed in-memory)
// 2) TableConfigCache (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfigCache. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) MergeColumnsFromDestination(destCols ...columns.Column) error {
	if t == nil || len(destCols) == 0 {
		return nil
	}

	for _, inMemoryCol := range t.inMemoryColumns.GetColumns() {
		var foundColumn columns.Column
		var found bool
		for _, destCol := range destCols {
			if destCol.Name() == strings.ToLower(inMemoryCol.Name()) {
				if destCol.KindDetails.Kind == typing.Invalid.Kind {
					return fmt.Errorf("column %q is invalid", destCol.Name())
				}

				foundColumn = destCol
				found = true
				break
			}
		}

		if found {
			t.inMemoryColumns.UpdateColumn(mergeColumn(inMemoryCol, foundColumn))
		}
	}

	return nil
}

// mergeColumn - This function will merge the in-memory column with the destination column.
func mergeColumn(inMemoryCol columns.Column, destCol columns.Column) columns.Column {
	inMemoryCol.KindDetails.Kind = destCol.KindDetails.Kind
	// Copy over backfilled
	inMemoryCol.SetBackfilled(destCol.Backfilled())

	// Copy over string precision, if it exists
	if destCol.KindDetails.OptionalStringPrecision != nil {
		inMemoryCol.KindDetails.OptionalStringPrecision = destCol.KindDetails.OptionalStringPrecision
	}

	// Copy over integer kind, if exists.
	if destCol.KindDetails.OptionalIntegerKind != nil {
		inMemoryCol.KindDetails.OptionalIntegerKind = destCol.KindDetails.OptionalIntegerKind
	}

	// If destination column details does have decimal details set and in-memory is not, then we should copy it over.
	if !destCol.KindDetails.DecimalDetailsNotSet() && inMemoryCol.KindDetails.DecimalDetailsNotSet() {
		inMemoryCol.KindDetails.ExtendedDecimalDetails = destCol.KindDetails.ExtendedDecimalDetails
	}

	// If the destination column does not have extended decimal details, we should remove it from the in-memory column as well
	if destCol.KindDetails.DecimalDetailsNotSet() && !inMemoryCol.KindDetails.DecimalDetailsNotSet() {
		inMemoryCol.KindDetails.ExtendedDecimalDetails = nil
	}

	return inMemoryCol
}
