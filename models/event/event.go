package event

import (
	"cmp"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/models"
)

type Event struct {
	Table         string
	PrimaryKeyMap map[string]any
	Data          map[string]any // json serialized column data

	OptionalSchema map[string]typing.KindDetails
	Columns        *columns.Columns
	Deleted        bool

	// When the database event was executed
	executionTime time.Time
	mode          config.Mode
}

func hashData(data map[string]any, tc kafkalib.TopicConfig) map[string]any {
	for _, columnToHash := range tc.ColumnsToHash {
		if value, isOk := data[columnToHash]; isOk {
			data[columnToHash] = cryptography.HashValue(value)
		}
	}

	return data
}

func ToMemoryEvent(event cdc.Event, pkMap map[string]any, tc kafkalib.TopicConfig, cfgMode config.Mode) (Event, error) {
	cols, err := event.GetColumns()
	if err != nil {
		return Event{}, err
	}
	// Now iterate over pkMap and tag each column that is a primary key
	if cols != nil {
		for primaryKey := range pkMap {
			err = cols.UpsertColumn(
				// We need to escape the column name similar to have parity with event.GetColumns()
				columns.EscapeName(primaryKey),
				columns.UpsertColumnArg{
					PrimaryKey: typing.ToPtr(true),
				},
			)

			if err != nil {
				return Event{}, fmt.Errorf("failed to upsert column: %w", err)
			}
		}
	}

	evtData, err := event.GetData(pkMap, tc)
	if err != nil {
		return Event{}, err
	}
	tblName := cmp.Or(tc.TableName, event.GetTableName())
	if cfgMode == config.History {
		if !strings.HasSuffix(tblName, constants.HistoryModeSuffix) {
			// History mode will include a table suffix and operation column
			tblName += constants.HistoryModeSuffix
			slog.Warn(fmt.Sprintf("History mode is enabled, but table name does not have a %s suffix, so we're adding it...", constants.HistoryModeSuffix), slog.String("tblName", tblName))
		}

		evtData[constants.OperationColumnMarker] = event.Operation()

		// We don't need the deletion markers either.
		delete(evtData, constants.DeleteColumnMarker)
		delete(evtData, constants.OnlySetDeleteColumnMarker)
	}

	optionalSchema, err := event.GetOptionalSchema()
	if err != nil {
		return Event{}, err
	}

	_event := Event{
		executionTime:  event.GetExecutionTime(),
		mode:           cfgMode,
		Table:          tblName,
		PrimaryKeyMap:  pkMap,
		OptionalSchema: optionalSchema,
		Columns:        cols,
		Data:           hashData(evtData, tc),
		Deleted:        event.DeletePayload(),
	}

	return _event, nil
}

// EmitExecutionTimeLag - This will check against the current time and the event execution time and emit the lag.
func (e *Event) EmitExecutionTimeLag(metricsClient base.Client) {
	metricsClient.GaugeWithSample(
		"row.execution_time_lag",
		float64(time.Since(e.executionTime).Milliseconds()),
		map[string]string{
			"mode":  e.mode.String(),
			"table": e.Table,
		}, 0.5)
}

func (e *Event) Validate() error {
	// Does it have a PK or table set?
	if stringutil.Empty(e.Table) {
		return fmt.Errorf("table name is empty")
	}

	if len(e.PrimaryKeyMap) == 0 {
		return fmt.Errorf("primary key map is empty")
	}

	if len(e.Data) == 0 {
		return fmt.Errorf("event has no data")
	}

	if e.mode == config.History {
		// History mode does not have the delete column marker.
		return nil
	}

	// Check if delete flag exists.
	if _, isOk := e.Data[constants.DeleteColumnMarker]; !isOk {
		return fmt.Errorf("delete column marker does not exist")
	}

	return nil
}

// PrimaryKeys is returned in a sorted manner to be safe.
// We use PrimaryKeyValue() as our internal identifier within our db
// It is critical to make sure `PrimaryKeyValue()` is a deterministic call.
func (e *Event) PrimaryKeys() []string {
	var keys []string
	for key := range e.PrimaryKeyMap {
		keys = append(keys, key)
	}

	slices.Sort(keys)
	return keys
}

// PrimaryKeyValue - as per above, this needs to return a deterministic k/v string.
func (e *Event) PrimaryKeyValue() string {
	var key string
	for _, pk := range e.PrimaryKeys() {
		key += fmt.Sprintf("%s=%v", pk, e.PrimaryKeyMap[pk])
	}

	return key
}

// Save will save the event into our in memory event
// It will return (flush bool, flushReason string, err error)
func (e *Event) Save(cfg config.Config, inMemDB *models.DatabaseData, tc kafkalib.TopicConfig, message artie.Message) (bool, string, error) {
	if err := e.Validate(); err != nil {
		return false, "", fmt.Errorf("event validation failed: %w", err)
	}

	// Does the table exist?
	td := inMemDB.GetOrCreateTableData(e.Table)
	td.Lock()
	defer td.Unlock()
	if td.Empty() {
		cols := &columns.Columns{}
		if e.Columns != nil {
			cols = e.Columns
		}

		td.SetTableData(optimization.NewTableData(cols, cfg.Mode, e.PrimaryKeys(), tc, e.Table))
	} else {
		if e.Columns != nil {
			// Iterate over this again just in case.
			for _, col := range e.Columns.GetColumns() {
				td.AddInMemoryCol(col)
			}
		}
	}

	// Table columns
	inMemoryColumns := td.ReadOnlyInMemoryCols()
	// Update col if necessary
	sanitizedData := make(map[string]any)
	for _col, val := range e.Data {
		newColName := columns.EscapeName(_col)
		if newColName != _col {
			// This means that the column name has changed.
			// We need to update the column name in the sanitizedData map.
			sanitizedData[newColName] = val
		}

		toastedCol := val == constants.ToastUnavailableValuePlaceholder
		if !toastedCol {
			// If the value is in map[string]string, the TOASTED value will look like map[__debezium_unavailable_value:__debezium_unavailable_value]
			valMap, isOk := val.(map[string]any)
			if isOk {
				if _, isOk = valMap[constants.ToastUnavailableValuePlaceholder]; isOk {
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
			retrievedColumn, isOk := inMemoryColumns.GetColumn(newColName)
			if !isOk {
				// This would only happen if the columns did not get passed in initially.
				kindDetails, err := typing.ParseValue(_col, e.OptionalSchema, val)
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
					kindDetails, err := typing.ParseValue(_col, e.OptionalSchema, val)
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
	e.Data = sanitizedData
	td.InsertRow(e.PrimaryKeyValue(), e.Data, e.Deleted)
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
	if message.Kind() == artie.Kafka {
		td.PartitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		td.PartitionsToLastMessage[message.Partition()] = append(td.PartitionsToLastMessage[message.Partition()], message)
	}

	td.LatestCDCTs = e.executionTime
	flush, flushReason := td.ShouldFlush(cfg)
	return flush, flushReason, nil
}
