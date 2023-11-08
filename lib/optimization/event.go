package optimization

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type TableData struct {
	inMemoryColumns *columns.Columns                  // list of columns
	rowsData        map[string]map[string]interface{} // pk -> { col -> val }
	primaryKeys     []string

	TopicConfig kafkalib.TopicConfig
	// Partition to the latest offset(s).
	// For Kafka, we only need the last message to commit the offset
	// However, pub/sub requires every single message to be acked
	PartitionsToLastMessage map[string][]artie.Message

	// This is used for the automatic schema detection
	LatestCDCTs time.Time
	approxSize  int
	// containOtherOperations - this means the `TableData` object contains other events that arises from CREATE, UPDATE, REPLICATION
	// if this value is false, that means it is only deletes. Which means we should not drop columns
	containOtherOperations bool

	temporaryTableSuffix string

	// Name of the table in the destination
	// Prefer calling .Name() everywhere
	name string
}

func (t *TableData) ContainOtherOperations() bool {
	return t.containOtherOperations
}

func (t *TableData) PrimaryKeys(ctx context.Context, args *sql.NameArgs) []columns.Wrapper {
	var primaryKeysEscaped []columns.Wrapper
	for _, pk := range t.primaryKeys {
		col := columns.NewColumn(pk, typing.Invalid)
		primaryKeysEscaped = append(primaryKeysEscaped, columns.NewWrapper(ctx, col, args))
	}

	return primaryKeysEscaped
}

func (t *TableData) Name(ctx context.Context, args *sql.NameArgs) string {
	return sql.EscapeName(ctx, t.name, args)
}

func (t *TableData) SetInMemoryColumns(columns *columns.Columns) {
	t.inMemoryColumns = columns
	return
}

func (t *TableData) AddInMemoryCol(column columns.Column) {
	t.inMemoryColumns.AddColumn(column)
	return
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

func NewTableData(inMemoryColumns *columns.Columns, primaryKeys []string, topicConfig kafkalib.TopicConfig, name string) *TableData {
	return &TableData{
		inMemoryColumns: inMemoryColumns,
		rowsData:        map[string]map[string]interface{}{},
		primaryKeys:     primaryKeys,
		TopicConfig:     topicConfig,
		// temporaryTableSuffix is being set in `ResetTempTableSuffix`
		temporaryTableSuffix:    "",
		PartitionsToLastMessage: map[string][]artie.Message{},
		name:                    stringutil.Override(name, topicConfig.TableName),
	}
}

// InsertRow creates a single entrypoint for how rows get added to TableData
// This is important to avoid concurrent r/w, but also the ability for us to add or decrement row size by keeping a running total
// With this, we are able to reduce the latency by 500x+ on a 5k row table. See event_bench_test.go vs. size_bench_test.go
func (t *TableData) InsertRow(pk string, rowData map[string]interface{}, delete bool) {
	var prevRowSize int
	prevRow, isOk := t.rowsData[pk]
	if isOk {
		prevRowSize = size.GetApproxSize(prevRow)
		for key, val := range rowData {
			if val == constants.ToastUnavailableValuePlaceholder {
				// Copy it from prevRow.
				prevVal, isOk := prevRow[key]
				if !isOk {
					continue
				}

				// If we got back a TOASTED value, we need to use the previous row.
				rowData[key] = prevVal
			}
		}
	}

	newRowSize := size.GetApproxSize(rowData)
	// If prevRow doesn't exist, it'll be 0, which is a no-op.
	t.approxSize += newRowSize - prevRowSize
	t.rowsData[pk] = rowData

	if !delete && !t.containOtherOperations {
		t.containOtherOperations = true
	}

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

func (t *TableData) ToFqName(ctx context.Context, kind constants.DestinationKind, escape bool) string {
	switch kind {
	case constants.S3:
		// S3 should be db.schema.tableName, but we don't need to escape, since it's not a SQL db.
		return fmt.Sprintf("%s.%s.%s", t.TopicConfig.Database, t.TopicConfig.Schema, t.Name(ctx, &sql.NameArgs{
			Escape:   false,
			DestKind: kind,
		}))
	case constants.Redshift:
		// Redshift is Postgres compatible, so when establishing a connection, we'll specify a database.
		// Thus, we only need to specify schema and table name here.
		return fmt.Sprintf("%s.%s", t.TopicConfig.Schema, t.Name(ctx, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	case constants.BigQuery:
		// The fully qualified name for BigQuery is: project_id.dataset.tableName.
		return fmt.Sprintf("%s.%s.%s", config.FromContext(ctx).Config.BigQuery.ProjectID, t.TopicConfig.Database, t.Name(ctx, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	default:
		return fmt.Sprintf("%s.%s.%s", t.TopicConfig.Database, t.TopicConfig.Schema, t.Name(ctx, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	}
}

func (t *TableData) Rows() uint {
	if t == nil {
		return 0
	}

	return uint(len(t.rowsData))
}

func (t *TableData) DistinctDates(ctx context.Context, colName string) ([]string, error) {
	retMap := make(map[string]bool)
	for _, row := range t.rowsData {
		val, isOk := row[colName]
		if !isOk {
			return nil, fmt.Errorf("col: %v does not exist on row: %v", colName, row)
		}

		extTime, err := ext.ParseFromInterface(ctx, val)
		if err != nil {
			return nil, fmt.Errorf("col: %v is not a time column, value: %v, err: %v", colName, val, err)
		}

		retMap[extTime.String(ext.PostgresDateFormat)] = true
	}

	var distinctDates []string
	for key := range retMap {
		distinctDates = append(distinctDates, key)
	}

	return distinctDates, nil
}

func (t *TableData) ResetTempTableSuffix() {
	if t == nil {
		// This is needed because we periodically wipe tableData
		return
	}

	t.temporaryTableSuffix = fmt.Sprintf("%s_%s", constants.ArtiePrefix, stringutil.Random(10))
}

func (t *TableData) TempTableSuffix() string {
	return t.temporaryTableSuffix
}

// ShouldFlush will return whether Transfer should flush
// If so, what is the reason?
func (t *TableData) ShouldFlush(ctx context.Context) (bool, string) {
	settings := config.FromContext(ctx)
	if t.Rows() > settings.Config.BufferRows {
		return true, "rows"
	}

	if t.approxSize > settings.Config.FlushSizeKb*1024 {
		return true, "size"
	}

	return false, ""
}

// UpdateInMemoryColumnsFromDestination - When running Transfer, we will have 2 column types.
// 1) TableData (constructed in-memory)
// 2) TableConfig (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfig. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) UpdateInMemoryColumnsFromDestination(ctx context.Context, cols ...columns.Column) {
	if t == nil {
		return
	}

	for _, inMemoryCol := range t.inMemoryColumns.GetColumns() {
		var foundColumn columns.Column
		var found bool
		for _, col := range cols {
			if col.Name(ctx, nil) == strings.ToLower(inMemoryCol.Name(ctx, nil)) {
				foundColumn = col
				found = true
				break
			}
		}

		if found {
			// If the inMemoryColumn is decimal and foundColumn is integer, don't copy it over.
			// This is because parsing NUMERIC(...) will return an INTEGER if there's no decimal point.
			// However, this will wipe the precision unit from the INTEGER which may cause integer overflow.
			shouldSkip := inMemoryCol.KindDetails.Kind == typing.EDecimal.Kind && foundColumn.KindDetails.Kind == typing.Integer.Kind
			if !shouldSkip {
				// We should take `kindDetails.kind` and `backfilled` from foundCol
				// We are not taking primaryKey and defaultValue because DWH does not have this information.
				// Note: If our in-memory column is `Invalid`, it would get skipped during merge. However, if the column exists in
				// the destination, we'll copy the type over. This is to make sure we don't miss batch updates where the whole column in the batch is NULL.
				inMemoryCol.KindDetails.Kind = foundColumn.KindDetails.Kind
			}

			inMemoryCol.SetBackfilled(foundColumn.Backfilled())
			if foundColumn.KindDetails.ExtendedTimeDetails != nil {
				if inMemoryCol.KindDetails.ExtendedTimeDetails == nil {
					inMemoryCol.KindDetails.ExtendedTimeDetails = &ext.NestedKind{}
				}

				// Don't have tcKind.ExtendedTimeDetails update the format since the DWH will not have that.
				inMemoryCol.KindDetails.ExtendedTimeDetails.Type = foundColumn.KindDetails.ExtendedTimeDetails.Type
			}

			if foundColumn.KindDetails.ExtendedDecimalDetails != nil && inMemoryCol.KindDetails.ExtendedDecimalDetails == nil {
				inMemoryCol.KindDetails.ExtendedDecimalDetails = foundColumn.KindDetails.ExtendedDecimalDetails
			}

			t.inMemoryColumns.UpdateColumn(inMemoryCol)
		}
	}

	return
}
