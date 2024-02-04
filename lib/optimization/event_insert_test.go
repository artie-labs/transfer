package optimization

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/stretchr/testify/assert"
)

func TestInsertRow_Toast(t *testing.T) {
	type testCaseStruct struct {
		name             string
		primaryKey       string
		rowsDataToUpdate []map[string]interface{}
		expectedFinalRow map[string]interface{}
	}

	testCases := []testCaseStruct{
		{
			name:       "happy path",
			primaryKey: "123",
			rowsDataToUpdate: []map[string]interface{}{
				{
					"foo":   "bar",
					"dusty": "the mini aussie",
					"artie": "transfer",
				},
			},
			expectedFinalRow: map[string]interface{}{
				"foo":   "bar",
				"dusty": "the mini aussie",
				"artie": "transfer",
			},
		},
		{
			name:       "row that is followed by a TOASTED value (we skip) and drops an old column",
			primaryKey: "123",
			rowsDataToUpdate: []map[string]interface{}{
				{
					"foo":                        "bar",
					"dusty":                      "the mini aussie",
					"artie":                      "transfer",
					"this_row_should_be_deleted": true,
				},
				{
					"foo":   "bar",
					"dusty": constants.ToastUnavailableValuePlaceholder,
					"artie": "transfer5",
				},
			},
			expectedFinalRow: map[string]interface{}{
				"foo":   "bar",
				"dusty": "the mini aussie",
				"artie": "transfer5",
			},
		},
		{
			name:       "row that starts with TOASTED value",
			primaryKey: "123",
			rowsDataToUpdate: []map[string]interface{}{
				{
					"foo":   "bar",
					"dusty": constants.ToastUnavailableValuePlaceholder,
					"artie": "transfer5",
				},
			},
			expectedFinalRow: map[string]interface{}{
				"foo":   "bar",
				"dusty": constants.ToastUnavailableValuePlaceholder,
				"artie": "transfer5",
			},
		},
		{
			name:       "row that starts with a TOASTED value, then another update comes in with a new value AND new column",
			primaryKey: "123",
			rowsDataToUpdate: []map[string]interface{}{
				{
					"foo":   "bar",
					"dusty": constants.ToastUnavailableValuePlaceholder,
					"artie": "transfer5",
				},
				{
					"foo":     "bar",
					"dusty":   "the aussie",
					"artie":   "transfer5",
					"new_col": true,
				},
			},
			expectedFinalRow: map[string]interface{}{
				"foo":     "bar",
				"dusty":   "the aussie",
				"artie":   "transfer5",
				"new_col": true,
			},
		},
	}

	for _, testCase := range testCases {
		// Wipe the table data per test run.
		td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
		for _, rowData := range testCase.rowsDataToUpdate {
			td.InsertRow(testCase.primaryKey, rowData, false)
		}

		var actualSize int
		for _, rowData := range td.RowsData() {
			actualSize += size.GetApproxSize(rowData)
		}

		assert.Equal(t, td.approxSize, actualSize, testCase.name)                                   // Check size calculation is accurate
		assert.Equal(t, td.rowsData[testCase.primaryKey], testCase.expectedFinalRow, testCase.name) // Check data accuracy
	}
}

func TestTableData_InsertRow(t *testing.T) {
	td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	assert.Equal(t, 0, int(td.NumberOfRows()))

	// See if we can add rows to the private method.
	td.RowsData()["foo"] = map[string]interface{}{
		"foo": "bar",
	}

	assert.Equal(t, 0, int(td.NumberOfRows()))

	// Now insert the right way.
	td.InsertRow("foo", map[string]interface{}{
		"foo": "bar",
	}, false)

	assert.Equal(t, 1, int(td.NumberOfRows()))
}

func TestTableData_InsertRowApproxSize(t *testing.T) {
	// In this test, we'll insert 1000 rows, update X and then delete Y
	// Does the size then match up? We will iterate over a map to take advantage of the in-deterministic ordering of a map
	// So we can test multiple updates, deletes, etc.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{}, "foo")
	numInsertRows := 1000
	numUpdateRows := 420
	numDeleteRows := 250

	for i := 0; i < numInsertRows; i++ {
		td.InsertRow(fmt.Sprint(i), map[string]interface{}{
			"foo":     "bar",
			"array":   []int{1, 2, 3, 4, 5},
			"boolean": true,
			"nested_object": map[string]interface{}{
				"nested": map[string]interface{}{
					"foo":  "bar",
					"true": false,
				},
			},
		}, false)
	}

	var updateCount int
	for updateKey := range td.RowsData() {
		updateCount += 1
		td.InsertRow(updateKey, map[string]interface{}{
			"foo": "foo",
			"bar": "bar",
		}, false)

		if updateCount > numUpdateRows {
			break
		}
	}

	var deleteCount int
	for deleteKey := range td.RowsData() {
		deleteCount += 1
		td.InsertRow(deleteKey, map[string]interface{}{
			"__artie_deleted": true,
		}, true)

		if deleteCount > numDeleteRows {
			break
		}
	}

	var actualSize int
	for _, rowData := range td.RowsData() {
		actualSize += size.GetApproxSize(rowData)
	}

	assert.Equal(t, td.approxSize, actualSize)
}
