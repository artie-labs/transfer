package event

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/models"
)

type Event struct {
	table          string
	tableID        cdc.TableID
	data           map[string]any // json serialized column data
	optionalSchema map[string]typing.KindDetails
	columns        *columns.Columns
	deleted        bool
	primaryKeys    []string

	// [executionTime] - The database timestamp for when the event was created.
	executionTime time.Time
	mode          config.Mode
}

func (e Event) GetTableID() cdc.TableID {
	return e.tableID
}

func (e Event) GetTable() string {
	return e.table
}

func transformData(data map[string]any, tc kafkalib.TopicConfig) map[string]any {
	for _, columnToHash := range tc.ColumnsToHash {
		if value, ok := data[columnToHash]; ok {
			data[columnToHash] = cryptography.HashValue(value)
		}
	}

	// Exclude certain columns
	for _, col := range tc.ColumnsToExclude {
		delete(data, col)
	}

	// If column inclusion is specified, then we need to include only the specified columns
	if len(tc.ColumnsToInclude) > 0 {
		filteredData := make(map[string]any)
		for _, col := range tc.ColumnsToInclude {
			if value, ok := data[col]; ok {
				filteredData[col] = value
			}
		}

		// Include Artie columns
		for _, col := range constants.ArtieColumns {
			if value, ok := data[col]; ok {
				filteredData[col] = value
			}
		}

		for _, col := range tc.StaticColumns {
			filteredData[col.Name] = col.Value
		}

		return filteredData
	}

	return data
}

func buildFilteredColumns(event cdc.Event, tc kafkalib.TopicConfig, reservedColumns []string) (*columns.Columns, error) {
	cols, err := event.GetColumns(reservedColumns)
	if err != nil {
		return nil, err
	}

	for _, col := range tc.ColumnsToExclude {
		cols.DeleteColumn(col)
	}

	if len(tc.ColumnsToInclude) > 0 {
		var filteredColumns columns.Columns
		for _, col := range tc.ColumnsToInclude {
			if existingColumn, ok := cols.GetColumn(col); ok {
				filteredColumns.AddColumn(existingColumn)
			}
		}

		for _, col := range constants.ArtieColumns {
			if existingColumn, ok := cols.GetColumn(col); ok {
				filteredColumns.AddColumn(existingColumn)
			}
		}

		// If columns to include is specified, we should always include static columns.
		for _, col := range tc.StaticColumns {
			filteredColumns.AddColumn(columns.NewColumn(col.Name, typing.String))
		}

		return &filteredColumns, nil
	}

	// Include static columns
	for _, col := range tc.StaticColumns {
		cols.AddColumn(columns.NewColumn(col.Name, typing.String))
	}

	return cols, nil
}

func buildPrimaryKeys(tc kafkalib.TopicConfig, pkMap map[string]any, reservedColumns []string) []string {
	var pks []string
	if len(tc.PrimaryKeysOverride) > 0 {
		for _, pk := range tc.PrimaryKeysOverride {
			pks = append(pks, columns.EscapeName(pk, reservedColumns))
		}

		return pks
	}

	// [pkMap] is already escaped.
	for pk := range pkMap {
		pks = append(pks, pk)
	}

	for _, pk := range tc.IncludePrimaryKeys {
		escapedPk := columns.EscapeName(pk, reservedColumns)
		if _, ok := pkMap[escapedPk]; !ok {
			pks = append(pks, escapedPk)
		}
	}

	return pks
}

func ToMemoryEvent(ctx context.Context, dest destination.Baseline, event cdc.Event, pkMap map[string]any, tc kafkalib.TopicConfig, cfgMode config.Mode) (Event, error) {
	var reservedColumns []string
	if _dest, ok := dest.(destination.Destination); ok {
		reservedColumns = _dest.Dialect().ReservedColumnNames()
	}

	cols, err := buildFilteredColumns(event, tc, reservedColumns)
	if err != nil {
		return Event{}, fmt.Errorf("failed to build filtered columns: %w", err)
	}

	pks := buildPrimaryKeys(tc, pkMap, reservedColumns)

	if cols != nil {
		// All keys in pks are already escaped, so don't escape again
		for _, pk := range pks {
			err = cols.UpsertColumn(
				pk,
				columns.UpsertColumnArg{
					PrimaryKey: typing.ToPtr(true),
				},
			)
			if err != nil {
				return Event{}, fmt.Errorf("failed to upsert column: %w", err)
			}
		}
	}

	evtData, err := event.GetData(tc)
	if err != nil {
		return Event{}, fmt.Errorf("failed to get data: %w", err)
	}

	if tc.IncludeArtieOperation {
		evtData[constants.OperationColumnMarker] = string(event.Operation())
	}

	if tc.IncludeSourceMetadata {
		metadata, err := event.GetSourceMetadata()
		if err != nil {
			return Event{}, fmt.Errorf("failed to get source metadata: %w", err)
		}

		evtData[constants.SourceMetadataColumnMarker] = metadata
		cols.AddColumn(columns.NewColumn(constants.SourceMetadataColumnMarker, typing.Struct))
	}

	if tc.IncludeFullSourceTableName {
		evtData[constants.FullSourceTableNameColumnMarker] = event.GetFullTableName()
	}

	tblName := cmp.Or(tc.TableName, event.GetTableName())

	if cfgMode == config.History {
		if !strings.HasSuffix(tblName, constants.HistoryModeSuffix) {
			// History mode will include a table suffix and operation column
			tblName += constants.HistoryModeSuffix
			slog.Warn(fmt.Sprintf("History mode is enabled, but table name does not have a %q suffix, so we're adding it...", constants.HistoryModeSuffix), slog.String("tblName", tblName))
		}

		// If this is already set, it's a no-op.
		evtData[constants.OperationColumnMarker] = string(event.Operation())

		// We don't need the deletion markers either.
		delete(evtData, constants.DeleteColumnMarker)
		delete(evtData, constants.OnlySetDeleteColumnMarker)
	} else if tc.SoftPartitioning.Enabled {
		// TODO: cache exact match or fix upstream to pass the column name from source table
		maybeDatetime, ok := maputil.GetCaseInsensitiveValue(evtData, tc.SoftPartitioning.PartitionColumn)
		if !ok {
			return Event{}, fmt.Errorf("partition column %q not found in data", tc.SoftPartitioning.PartitionColumn)
		}
		actuallyDateTime, err := typing.ParseTimestampTZFromAny(maybeDatetime)
		if err != nil {
			return Event{}, fmt.Errorf("failed to assert datetime: %w for table %q schema %q", err, tc.TableName, tc.Schema)
		}
		// TODO: clean up parameters, i.e. ctx, dest, etc
		suffix, err := BuildSoftPartitionSuffix(ctx, tc, actuallyDateTime, event.GetExecutionTime(), tblName, dest)
		if err != nil {
			return Event{}, fmt.Errorf("failed to calculate soft partition suffix: %w", err)
		}
		tblName = tblName + suffix
	}

	optionalSchema, err := event.GetOptionalSchema()
	if err != nil {
		return Event{}, fmt.Errorf("failed to get optional schema: %w", err)
	}

	// Static columns cannot collide with the event data.
	for _, staticColumn := range tc.StaticColumns {
		if _, ok := evtData[staticColumn.Name]; ok {
			return Event{}, fmt.Errorf("static column %q collides with event data", staticColumn.Name)
		}

		// Inject static columns into the event data.
		evtData[staticColumn.Name] = staticColumn.Value
	}

	sort.Strings(pks)
	return Event{
		executionTime: event.GetExecutionTime(),
		mode:          cfgMode,
		// [primaryKeys] needs to be sorted so that we have a deterministic way to identify a row in our in-memory db.
		primaryKeys:    pks,
		table:          tblName,
		tableID:        cdc.NewTableID(tc.Schema, tblName),
		optionalSchema: optionalSchema,
		columns:        cols,
		data:           transformData(evtData, tc),
		deleted:        event.DeletePayload(),
	}, nil
}

// GetData - This will return the data for the event.
func (e Event) GetData() map[string]any {
	return e.data
}

// SetData - This will set the data for the event. This is used by Reader.
func (e *Event) SetData(key string, value any) {
	e.data[key] = value
}

// EmitExecutionTimeLag - This will check against the current time and the event execution time and emit the lag.
func (e *Event) EmitExecutionTimeLag(metricsClient base.Client) {
	metricsClient.GaugeWithSample(
		"row.execution_time_lag",
		float64(time.Since(e.executionTime).Milliseconds()),
		map[string]string{
			"mode":  e.mode.String(),
			"table": e.table,
		}, 0.5)
}

func (e *Event) Validate() error {
	// Does it have a PK or table set?
	if stringutil.Empty(e.table) {
		return fmt.Errorf("table name is empty")
	}

	if len(e.primaryKeys) == 0 {
		return fmt.Errorf("primary keys are empty")
	}

	if len(e.data) == 0 {
		return fmt.Errorf("event has no data")
	}

	if e.mode == config.History {
		// History mode does not have the delete column marker.
		return nil
	}

	// Check if delete flag exists.
	if _, ok := e.data[constants.DeleteColumnMarker]; !ok {
		return fmt.Errorf("delete column marker does not exist")
	}

	return nil
}

func (e *Event) GetPrimaryKeys() []string {
	return e.primaryKeys
}

// PrimaryKeyValue - as per above, this needs to return a deterministic k/v string.
// Must only call this after the event data has been sanitized within [event.Save].
func (e *Event) PrimaryKeyValue() (string, error) {
	var key string
	for _, pk := range e.GetPrimaryKeys() {
		value, ok := e.data[pk]
		if !ok {
			return "", fmt.Errorf("primary key %q not found in data: %v", pk, e.data)
		}

		key += fmt.Sprintf("%s=%v", pk, value)
	}

	return key, nil
}

// Save will save the event into our in memory event
// It will return (flush bool, flushReason string, err error)
func (e *Event) Save(cfg config.Config, inMemDB *models.DatabaseData, tc kafkalib.TopicConfig, reservedColumns []string) (bool, string, error) {
	if err := e.Validate(); err != nil {
		return false, "", fmt.Errorf("event validation failed: %w", err)
	}

	// Does the table exist?
	td := inMemDB.GetOrCreateTableData(e.tableID, tc.Topic)
	if td.Empty() {
		cols := &columns.Columns{}
		if e.columns != nil {
			cols = e.columns
		}

		td.SetTableData(optimization.NewTableData(cols, cfg.Mode, e.GetPrimaryKeys(), tc, e.table))
	} else {
		if e.columns != nil {
			// Iterate over this again just in case.
			for _, col := range e.columns.GetColumns() {
				td.AddInMemoryCol(col)
			}
		}
	}

	// Table columns
	inMemoryColumns := td.ReadOnlyInMemoryCols()
	// Update col if necessary
	sanitizedData := make(map[string]any)
	for _col, val := range e.data {
		newColName := columns.EscapeName(_col, reservedColumns)
		if newColName != _col {
			// This means that the column name has changed.
			// We need to update the column name in the sanitizedData map.
			sanitizedData[newColName] = val
		}

		toastedCol := val == constants.ToastUnavailableValuePlaceholder
		if !toastedCol {
			// If the value is in map[string]string, the TOASTED value will look like map[__debezium_unavailable_value:__debezium_unavailable_value]
			valMap, ok := val.(map[string]any)
			if ok {
				if _, ok = valMap[constants.ToastUnavailableValuePlaceholder]; ok {
					// Casting the value back into how other TOASTED values look like.
					val = constants.ToastUnavailableValuePlaceholder
					toastedCol = true
				}
			}
		}

		if toastedCol {
			err := inMemoryColumns.UpsertColumn(newColName, columns.UpsertColumnArg{
				ToastCol: typing.ToPtr(true),
			})
			if err != nil {
				return false, "", fmt.Errorf("failed to upsert column: %w", err)
			}
		} else {
			retrievedColumn, ok := inMemoryColumns.GetColumn(newColName)
			if !ok {
				// This would only happen if the columns did not get passed in initially.
				kindDetails, err := typing.ParseValue(_col, e.optionalSchema, val)
				if err != nil {
					return false, "", fmt.Errorf("failed to parse value: %w", err)
				}

				inMemoryColumns.AddColumn(columns.NewColumn(newColName, kindDetails))
			} else {
				if retrievedColumn.KindDetails == typing.Invalid {
					// If colType is Invalid, let's see if we can update it to a better type
					// If everything is nil, we don't need to add a column
					// However, it's important to create a column even if it's nil.
					// This is because we don't want to think that it's okay to drop a column in DWH
					kindDetails, err := typing.ParseValue(_col, e.optionalSchema, val)
					if err != nil {
						return false, "", fmt.Errorf("failed to parse value: %w", err)
					}

					if kindDetails.Kind != typing.Invalid.Kind {
						retrievedColumn.KindDetails = kindDetails
						inMemoryColumns.UpdateColumn(retrievedColumn)
					}
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Now we commit the table columns.
	td.SetInMemoryColumns(inMemoryColumns)

	// Swap out sanitizedData <> data.
	e.data = sanitizedData

	pkValueString, err := e.PrimaryKeyValue()
	if err != nil {
		return false, "", fmt.Errorf("failed to retrieve primary key value: %w", err)
	}

	td.InsertRow(pkValueString, e.data, e.deleted)
	td.SetLatestTimestamp(e.executionTime)
	flush, flushReason := td.ShouldFlush(cfg)
	return flush, flushReason, nil
}

func BuildSoftPartitionSuffix(
	ctx context.Context,
	tc kafkalib.TopicConfig,
	partitionColumnValue time.Time,
	executionTime time.Time,
	tblName string,
	dest destination.Baseline,
) (string, error) {
	if !tc.SoftPartitioning.Enabled {
		return "", nil
	}
	suffix, err := tc.SoftPartitioning.PartitionFrequency.Suffix(partitionColumnValue)
	if err != nil {
		return "", fmt.Errorf("failed to get partition frequency suffix: %w for table %q schema %q", err, tc.TableName, tc.Schema)
	}
	// only works for full destinations, not just Baseline
	if destWithTableConfig, ok := dest.(destination.Destination); ok {
		// Check if we should write to compacted table
		sp := tc.SoftPartitioning
		if sp.PartitionFrequency == "" {
			return "", fmt.Errorf("partition frequency is required")
		}
		distance := sp.PartitionFrequency.PartitionDistance(partitionColumnValue, executionTime)
		if distance == 0 {
			// Same partition, use base suffix
		} else if distance < 0 {
			return "", fmt.Errorf("partition time %v for column %q is in the future of execution time %v", partitionColumnValue, sp.PartitionColumn, executionTime)
		} else {
			partitionedTableName := tblName + suffix
			tableID := dest.IdentifierFor(kafkalib.DatabaseAndSchemaPair{Database: tc.Database, Schema: tc.Schema}, partitionedTableName)
			tableConfig, err := destWithTableConfig.GetTableConfig(ctx, tableID, false)
			if err != nil {
				return "", fmt.Errorf("failed to get table config: %w", err)
			}
			// tableConfig.CreateTable() will return true if the table doesn't exist.
			if tableConfig.CreateTable() {
				suffix = kafkalib.CompactedTableSuffix
			}
		}
	}
	return suffix, nil
}
