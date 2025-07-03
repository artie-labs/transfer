package optimization

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestTableData_WipeData(t *testing.T) {
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	td.containsHardDeletes = true

	assert.True(t, td.ContainsHardDeletes())

	// After we wipe the table data, hard delete flag should stick
	td.WipeData()
	assert.True(t, td.ContainsHardDeletes())
}

func TestTableData_ReadOnlyInMemoryCols(t *testing.T) {
	// Making sure the columns are actually read only.
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("name", typing.String))

	td := NewTableData(&cols, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	readOnlyCols := td.ReadOnlyInMemoryCols()
	readOnlyCols.AddColumn(columns.NewColumn("last_name", typing.String))

	// Check if last_name actually exists.
	_, isOk := td.ReadOnlyInMemoryCols().GetColumn("last_name")
	assert.False(t, isOk)

	// Check length is 1.
	assert.Equal(t, 1, len(td.ReadOnlyInMemoryCols().GetColumns()))
}

func TestTableData_UpdateInMemoryColumns(t *testing.T) {
	var _cols columns.Columns
	for colName, colKind := range map[string]typing.KindDetails{
		"FOO":       typing.String,
		"bar":       typing.Invalid,
		"CHANGE_me": typing.String,
	} {
		_cols.AddColumn(columns.NewColumn(colName, colKind))
	}

	tableData := &TableData{
		inMemoryColumns: &_cols,
	}

	for name, colKindDetails := range map[string]typing.KindDetails{
		"foo":       typing.String,
		"change_me": typing.TimestampTZ,
		"bar":       typing.Boolean,
	} {
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn(name, colKindDetails)))
	}

	// It's saved back in the original format.
	_, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("foo")
	assert.False(t, isOk)

	_, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("FOO")
	assert.True(t, isOk)

	col, isOk := tableData.ReadOnlyInMemoryCols().GetColumn("CHANGE_me")
	assert.True(t, isOk)
	assert.Equal(t, typing.TimestampTZ, col.KindDetails)

	// It went from invalid to boolean.
	col, isOk = tableData.ReadOnlyInMemoryCols().GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Boolean, col.KindDetails)
}

func TestTableData_ShouldFlushRowLength(t *testing.T) {
	cfg := config.Config{
		FlushSizeKb: 500,
		BufferRows:  2,
	}

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	for i := 0; i < 3; i++ {
		shouldFlush, flushReason := td.ShouldFlush(cfg)
		assert.False(t, shouldFlush)
		assert.Empty(t, flushReason)

		td.InsertRow(fmt.Sprint(i), map[string]any{
			"foo": "bar",
		}, false)
	}

	shouldFlush, flushReason := td.ShouldFlush(cfg)
	assert.True(t, shouldFlush)
	assert.Equal(t, "rows", flushReason)
}

func TestTableData_ContainsHardDeletes(t *testing.T) {
	{
		// Hard delete = true
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))

		td.InsertRow("123", map[string]any{"id": "123"}, true)
		assert.Equal(t, 1, int(td.NumberOfRows()))

		assert.True(t, td.ContainsHardDeletes())
	}
	{
		// TopicConfig has soft delete turned on, so hard delete = false
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))

		td.InsertRow("123", map[string]any{"id": "123"}, true)
		assert.Equal(t, 1, int(td.NumberOfRows()))
		assert.False(t, td.ContainsHardDeletes())
	}
}

func TestTableData_ShouldFlushRowSize(t *testing.T) {
	cfg := config.Config{
		FlushSizeKb: 5,
		BufferRows:  20000,
	}

	// Insert 3 rows and confirm that we need to flush.
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	for i := 0; i < 100; i++ {
		shouldFlush, flushReason := td.ShouldFlush(cfg)
		assert.False(t, shouldFlush)
		assert.Empty(t, flushReason)
		td.InsertRow(fmt.Sprint(i), map[string]any{
			"foo":   "bar",
			"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
			"true":  true,
			"false": false,
			"nested": map[string]any{
				"foo": "bar",
			},
		}, false)
	}

	td.InsertRow("33333", map[string]any{
		"foo":   "bar",
		"array": []string{"foo", "bar", "dusty", "the aussie", "robin", "jacqueline", "charlie"},
		"true":  true,
		"false": false,
		"nested": map[string]any{
			"foo": "bar",
			"bar": "xyz",
			"123": "9222213213j1i31j3k21j321k3j1k31jk31213123213213121322j31k2",
		},
	}, false)

	shouldFlush, flushReason := td.ShouldFlush(cfg)
	assert.True(t, shouldFlush)
	assert.Equal(t, "size", flushReason)
}

func TestTableData_InsertRowIntegrity(t *testing.T) {
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "foo")
	assert.Equal(t, 0, int(td.NumberOfRows()))
	assert.False(t, td.ContainOtherOperations())

	for i := 0; i < 100; i++ {
		td.InsertRow("123", map[string]any{"id": "123"}, true)
		assert.False(t, td.ContainOtherOperations())
	}

	for i := 0; i < 100; i++ {
		td.InsertRow("123", map[string]any{"id": "123"}, false)
		assert.True(t, td.ContainOtherOperations())
	}
}

func TestTableData_InsertRowSoftDelete(t *testing.T) {
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
	assert.Equal(t, 0, int(td.NumberOfRows()))

	td.InsertRow("123", map[string]any{"id": "123", "name": "dana", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	assert.Equal(t, "dana", td.Rows()[0].GetData()["name"])

	td.InsertRow("123", map[string]any{"id": "123", "name": "dana2", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	assert.Equal(t, "dana2", td.Rows()[0].GetData()["name"])

	td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	// The previous value should be preserved, along with the delete marker
	assert.Equal(t, "dana2", td.Rows()[0].GetData()["name"])
	assert.Equal(t, true, td.Rows()[0].GetData()[constants.DeleteColumnMarker])
	// OnlySetDeleteColumnMarker should be false because we want to set the previously received values that haven't been flushed yet
	assert.Equal(t, false, td.Rows()[0].GetData()[constants.OnlySetDeleteColumnMarker])

	// Ensure two deletes in a row are handled idempotently (in case the delete event is sent twice)
	td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	assert.Equal(t, "dana2", td.Rows()[0].GetData()["name"])
	assert.Equal(t, true, td.Rows()[0].GetData()[constants.DeleteColumnMarker])
	assert.Equal(t, false, td.Rows()[0].GetData()[constants.OnlySetDeleteColumnMarker])
	{
		// If deleting a row we don't have in memory, OnlySetDeleteColumnMarker should stay true
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		assert.Equal(t, true, td.Rows()[0].GetData()[constants.OnlySetDeleteColumnMarker])
		// Two deletes in a row; OnlySetDeleteColumnMarker should still be true because we don't have the other values in memory
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		assert.Equal(t, true, td.Rows()[0].GetData()[constants.OnlySetDeleteColumnMarker])
	}
	{
		// If a row is created and deleted, then another row with the same primary key is created, the previous values should not be used
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))
		td.InsertRow("123", map[string]any{"id": "123", "name": "dana", "foo": "abc", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		td.InsertRow("123", map[string]any{"id": "123", "name": "dana-new", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
		assert.Equal(t, "dana-new", td.Rows()[0].GetData()["name"])
		assert.Nil(t, td.Rows()[0].GetData()["foo"])
		assert.Equal(t, false, td.Rows()[0].GetData()[constants.DeleteColumnMarker])
	}
}

func TestMergeColumn(t *testing.T) {
	{
		// Make sure it copies the kind over
		col := mergeColumn(columns.NewColumn("foo", typing.String), columns.NewColumn("foo", typing.Boolean))
		assert.Equal(t, typing.Boolean, col.KindDetails)
	}
	{
		// Make sure it copies the backfill over
		backfilledCol := columns.NewColumn("foo", typing.String)
		backfilledCol.SetBackfilled(true)
		cols := mergeColumn(columns.NewColumn("foo", typing.String), backfilledCol)
		assert.True(t, cols.Backfilled())
	}
	{
		// Make sure the string precision gets copied over
		columnWithStringPrecision := columns.NewColumn("foo", typing.String)
		columnWithStringPrecision.KindDetails.OptionalStringPrecision = typing.ToPtr(int32(5))
		col := mergeColumn(columns.NewColumn("foo", typing.String), columnWithStringPrecision)
		assert.Equal(t, int32(5), *col.KindDetails.OptionalStringPrecision)
	}
	{
		// Integer kind gets copied over
		intCol := columns.NewColumn("foo", typing.Integer)
		intCol.KindDetails.OptionalIntegerKind = typing.ToPtr(typing.SmallIntegerKind)
		col := mergeColumn(columns.NewColumn("foo", typing.String), intCol)
		assert.Equal(t, typing.SmallIntegerKind, *col.KindDetails.OptionalIntegerKind)
	}
	{
		// Decimal details
		{
			// Decimal details get copied over from destination column
			decimalCol := columns.NewColumn("foo", typing.EDecimal)
			details := decimal.NewDetails(5, 2)
			decimalCol.KindDetails.ExtendedDecimalDetails = &details

			col := mergeColumn(columns.NewColumn("foo", typing.String), decimalCol)
			assert.Equal(t, details, *col.KindDetails.ExtendedDecimalDetails)
		}
		{
			// Decimal details should get copied from destination column (in-memory column is not set)
			decimalCol := columns.NewColumn("foo", typing.EDecimal)
			destinationColumnDetails := decimal.NewDetails(5, 2)
			decimalCol.KindDetails.ExtendedDecimalDetails = &destinationColumnDetails

			inMemoryCol := columns.NewColumn("foo", typing.EDecimal)
			inMemoryDetails := decimal.NewDetails(decimal.PrecisionNotSpecified, decimal.DefaultScale)
			inMemoryCol.KindDetails.ExtendedDecimalDetails = &inMemoryDetails

			col := mergeColumn(inMemoryCol, decimalCol)
			assert.Equal(t, destinationColumnDetails, *col.KindDetails.ExtendedDecimalDetails)
		}
		{
			// Decimal details should be removed when destination column doesn't have them
			inMemoryCol := columns.NewColumn("foo", typing.EDecimal)
			details := decimal.NewDetails(5, 2)
			inMemoryCol.KindDetails.ExtendedDecimalDetails = &details

			destCol := columns.NewColumn("foo", typing.EDecimal)
			col := mergeColumn(inMemoryCol, destCol)
			assert.Nil(t, col.KindDetails.ExtendedDecimalDetails)
		}
	}
	{
		// Time details get copied over
		{
			// Testing for backwards compatibility
			// in-memory column is TimestampNTZ, destination column is TimestampTZ
			timestampNTZColumn := columns.NewColumn("foo", typing.TimestampNTZ)
			timestampTZColumn := columns.NewColumn("foo", typing.TimestampTZ)
			col := mergeColumn(timestampNTZColumn, timestampTZColumn)
			assert.Equal(t, typing.TimestampTZ, col.KindDetails)
		}
	}
}

func TestTableData_BuildColumnsToKeep(t *testing.T) {
	{
		// Nothing except history mode should give us the operation column
		td := TableData{mode: config.History}
		assert.ElementsMatch(t, []string{constants.OperationColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// If history mode and include artie operation are both true, we should only get the operation column once
		td := TableData{mode: config.History, topicConfig: kafkalib.TopicConfig{IncludeArtieOperation: true}}
		assert.ElementsMatch(t, []string{constants.OperationColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// Soft delete is enabled
		td := TableData{mode: config.Replication, topicConfig: kafkalib.TopicConfig{SoftDelete: true}}
		assert.ElementsMatch(t, []string{constants.DeleteColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// Artie + DB updated at are both true
		td := TableData{mode: config.Replication, topicConfig: kafkalib.TopicConfig{IncludeArtieUpdatedAt: true, IncludeDatabaseUpdatedAt: true}}
		assert.ElementsMatch(t, []string{constants.UpdateColumnMarker, constants.DatabaseUpdatedColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// Include artie operation is true
		td := TableData{mode: config.Replication, topicConfig: kafkalib.TopicConfig{IncludeArtieOperation: true}}
		assert.ElementsMatch(t, []string{constants.OperationColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// Include source metadata is true
		td := TableData{mode: config.Replication, topicConfig: kafkalib.TopicConfig{IncludeSourceMetadata: true}}
		assert.ElementsMatch(t, []string{constants.SourceMetadataColumnMarker}, td.BuildColumnsToKeep())
	}
	{
		// Include full source table name is true
		td := TableData{mode: config.Replication, topicConfig: kafkalib.TopicConfig{IncludeFullSourceTableName: true}}
		assert.ElementsMatch(t, []string{constants.FullSourceTableNameColumnMarker}, td.BuildColumnsToKeep())
	}
}
