package optimization

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestDistinctDates(t *testing.T) {
	testCases := []struct {
		name                string
		rowData             map[string]map[string]any // pk -> { col -> val }
		expectedErr         string
		expectedDatesString []string
	}{
		{
			name: "no dates",
		},
		{
			name: "one date",
			rowData: map[string]map[string]any{
				"1": {
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01"},
		},
		{
			name: "two dates",
			rowData: map[string]map[string]any{
				"1": {
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				"2": {
					"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01", "2020-01-02"},
		},
		{
			name: "3 dates, 2 unique",
			rowData: map[string]map[string]any{
				"1": {
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				"1_duplicate": {
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				"2": {
					"ts": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
			},
			expectedDatesString: []string{"2020-01-01", "2020-01-02"},
		},
		{
			name: "two dates, one is nil",
			rowData: map[string]map[string]any{
				"1": {
					"ts": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				},
				"2": {
					"ts": nil,
				},
			},
			expectedErr: "col: ts is not a time column",
		},
	}

	for _, testCase := range testCases {
		td := &TableData{
			rowsData: testCase.rowData,
		}

		actualValues, actualErr := td.DistinctDates("ts")
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, actualErr, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
			assert.Equal(t, true, slicesEqualUnordered(testCase.expectedDatesString, actualValues),
				fmt.Sprintf("2 arrays not the same, test name: %s, expected array: %v, actual array: %v",
					testCase.name, testCase.expectedDatesString, actualValues))
		}
	}
}

func slicesEqualUnordered(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	slices.Sort(s1)
	slices.Sort(s2)

	for i, v := range s1 {
		if v != s2[i] {
			return false
		}
	}

	return true
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
	assert.Equal(t, "dana", td.Rows()[0]["name"])

	td.InsertRow("123", map[string]any{"id": "123", "name": "dana2", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	assert.Equal(t, "dana2", td.Rows()[0]["name"])

	td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	// The previous value should be preserved, along with the delete marker
	assert.Equal(t, "dana2", td.Rows()[0]["name"])
	assert.Equal(t, true, td.Rows()[0][constants.DeleteColumnMarker])
	// OnlySetDeleteColumnMarker should be false because we want to set the previously received values that haven't been flushed yet
	assert.Equal(t, false, td.Rows()[0][constants.OnlySetDeleteColumnMarker])

	// Ensure two deletes in a row are handled idempotently (in case the delete event is sent twice)
	td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
	assert.Equal(t, 1, int(td.NumberOfRows()))
	assert.Equal(t, "dana2", td.Rows()[0]["name"])
	assert.Equal(t, true, td.Rows()[0][constants.DeleteColumnMarker])
	assert.Equal(t, false, td.Rows()[0][constants.OnlySetDeleteColumnMarker])

	{
		// If deleting a row we don't have in memory, OnlySetDeleteColumnMarker should stay true
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		assert.Equal(t, true, td.Rows()[0][constants.OnlySetDeleteColumnMarker])
		// Two deletes in a row; OnlySetDeleteColumnMarker should still be true because we don't have the other values in memory
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		assert.Equal(t, true, td.Rows()[0][constants.OnlySetDeleteColumnMarker])
	}

	{
		// If a row is created and deleted, then another row with the same primary key is created, the previous values should not be used
		td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{SoftDelete: true}, "foo")
		assert.Equal(t, 0, int(td.NumberOfRows()))
		td.InsertRow("123", map[string]any{"id": "123", "name": "dana", "foo": "abc", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
		td.InsertRow("123", map[string]any{"id": "123", constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true}, true)
		td.InsertRow("123", map[string]any{"id": "123", "name": "dana-new", constants.DeleteColumnMarker: false, constants.OnlySetDeleteColumnMarker: false}, false)
		assert.Equal(t, "dana-new", td.Rows()[0]["name"])
		assert.Nil(t, td.Rows()[0]["foo"])
		assert.Equal(t, false, td.Rows()[0][constants.DeleteColumnMarker])
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
		// Decimal details get copied over
		decimalCol := columns.NewColumn("foo", typing.EDecimal)
		details := decimal.NewDetails(5, 2)
		decimalCol.KindDetails.ExtendedDecimalDetails = &details

		col := mergeColumn(columns.NewColumn("foo", typing.String), decimalCol)
		assert.Equal(t, details, *col.KindDetails.ExtendedDecimalDetails)
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
