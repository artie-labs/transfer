package optimization

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type TableData struct {
	mode            config.Mode
	inMemoryColumns *columns.Columns // list of columns

	// rowsData is used for replication
	rowsData map[string]map[string]any // pk -> { col -> val }
	// rows is used for history mode, since it's append only.
	rows []map[string]any

	primaryKeys []string

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

	// containsHardDeletes - this means there are hard deletes in `rowsData`, so for multi-part merge statements, we should include a DELETE SQL statement.
	containsHardDeletes bool

	temporaryTableSuffix string

	// Name of the table in the destination
	// Prefer calling .Name() everywhere
	name string
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

func (t *TableData) PrimaryKeys(uppercaseEscNames bool, args *sql.NameArgs) []columns.Wrapper {
	var primaryKeysEscaped []columns.Wrapper
	for _, pk := range t.primaryKeys {
		col := columns.NewColumn(pk, typing.Invalid)
		primaryKeysEscaped = append(primaryKeysEscaped, columns.NewWrapper(col, uppercaseEscNames, args))
	}

	return primaryKeysEscaped
}

func (t *TableData) RawName() string {
	return t.name
}

func (t *TableData) Name(uppercaseEscNames bool, args *sql.NameArgs) string {
	return sql.EscapeName(t.name, uppercaseEscNames, args)
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

func NewTableData(inMemoryColumns *columns.Columns, mode config.Mode, primaryKeys []string, topicConfig kafkalib.TopicConfig, name string) *TableData {
	return &TableData{
		mode:            mode,
		inMemoryColumns: inMemoryColumns,
		rowsData:        map[string]map[string]any{},
		primaryKeys:     primaryKeys,
		TopicConfig:     topicConfig,
		// temporaryTableSuffix is being set in `ResetTempTableSuffix`
		temporaryTableSuffix:    "",
		PartitionsToLastMessage: map[string][]artie.Message{},
		name:                    name,
	}
}

// InsertRow creates a single entrypoint for how rows get added to TableData
// This is important to avoid concurrent r/w, but also the ability for us to add or decrement row size by keeping a running total
// With this, we are able to reduce the latency by 500x+ on a 5k row table. See event_bench_test.go vs. size_bench_test.go
func (t *TableData) InsertRow(pk string, rowData map[string]any, delete bool) {
	if t.mode == config.History {
		t.rows = append(t.rows, rowData)
		t.approxSize += size.GetApproxSize(rowData)
		return
	}

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

	// If there's an actual hard delete, let's update it.
	// We know because we have a delete operation and this topic is not configured to do soft deletes.
	if !t.containsHardDeletes && !t.TopicConfig.SoftDelete && delete {
		t.containsHardDeletes = true
	}
}

// Rows returns a read only slice of tableData's rows or rowsData depending on mode
func (t *TableData) Rows() []map[string]any {
	var rows []map[string]any

	if t.Mode() == config.History {
		// History mode, the data is stored under `rows`
		rows = append(rows, t.rows...)
	} else {
		for _, v := range t.rowsData {
			rows = append(rows, v)
		}
	}

	return rows
}

// RowsData will create a read-only map of `rowsData`, this should be strictly used for testing only.
func (t *TableData) RowsData() map[string]map[string]any {
	return maps.Clone(t.rowsData)
}

type FqNameOpts struct {
	BigQueryProjectID   string
	MsSQLSchemaOverride string
}

func (t *TableData) ToFqName(kind constants.DestinationKind, escape bool, uppercaseEscNames bool, opts FqNameOpts) string {
	switch kind {
	case constants.S3:
		// S3 should be db.schema.tableName, but we don't need to escape, since it's not a SQL db.
		return fmt.Sprintf("%s.%s.%s", t.TopicConfig.Database, t.TopicConfig.Schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   false,
			DestKind: kind,
		}))
	case constants.Redshift:
		// Redshift is Postgres compatible, so when establishing a connection, we'll specify a database.
		// Thus, we only need to specify schema and table name here.
		return fmt.Sprintf("%s.%s", t.TopicConfig.Schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	case constants.MSSQL:
		return fmt.Sprintf("%s.%s", stringutil.Override(t.TopicConfig.Schema, opts.MsSQLSchemaOverride), t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	case constants.BigQuery:
		// The fully qualified name for BigQuery is: project_id.dataset.tableName.
		return fmt.Sprintf("%s.%s.%s", opts.BigQueryProjectID, t.TopicConfig.Database, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	default:
		return fmt.Sprintf("%s.%s.%s", t.TopicConfig.Database, t.TopicConfig.Schema, t.Name(uppercaseEscNames, &sql.NameArgs{
			Escape:   escape,
			DestKind: kind,
		}))
	}
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

func (t *TableData) DistinctDates(colName string, additionalDateFmts []string) ([]string, error) {
	retMap := make(map[string]bool)
	for _, row := range t.rowsData {
		val, isOk := row[colName]
		if !isOk {
			return nil, fmt.Errorf("col: %v does not exist on row: %v", colName, row)
		}

		extTime, err := ext.ParseFromInterface(val, additionalDateFmts)
		if err != nil {
			return nil, fmt.Errorf("col: %v is not a time column, value: %v, err: %w", colName, val, err)
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

	t.temporaryTableSuffix = fmt.Sprintf("%s_%s", constants.ArtiePrefix, stringutil.Random(5))
}

func (t *TableData) TempTableSuffix() string {
	return fmt.Sprintf("%s_%d", t.temporaryTableSuffix, time.Now().Add(constants.TemporaryTableTTL).Unix())
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

// MergeColumnsFromDestination - When running Transfer, we will have 2 column types.
// 1) TableData (constructed in-memory)
// 2) TableConfig (coming from the SQL DESCRIBE or equivalent statement) from the destination
// Prior to merging, we will need to treat `tableConfig` as the source-of-truth and whenever there's discrepancies
// We will prioritize using the values coming from (2) TableConfig. We also cannot simply do a replacement, as we have in-memory columns
// That carry metadata for Artie Transfer. They are prefixed with __artie.
func (t *TableData) MergeColumnsFromDestination(destCols ...columns.Column) {
	if t == nil || len(destCols) == 0 {
		return
	}

	for _, inMemoryCol := range t.inMemoryColumns.GetColumns() {
		var foundColumn columns.Column
		var found bool
		for _, destCol := range destCols {
			if destCol.RawName() == strings.ToLower(inMemoryCol.RawName()) {
				foundColumn = destCol
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
				if foundColumn.KindDetails.OptionalStringPrecision != nil {
					inMemoryCol.KindDetails.OptionalStringPrecision = foundColumn.KindDetails.OptionalStringPrecision
				}
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
}
