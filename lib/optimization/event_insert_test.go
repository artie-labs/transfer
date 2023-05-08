package optimization

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableData_InsertRow(t *testing.T) {
	td := NewTableData(nil, nil, kafkalib.TopicConfig{})
	assert.Equal(t, 0, int(td.Rows()))

	// See if we can add rows to the private method.
	td.RowsData()["foo"] = map[string]interface{}{
		"foo": "bar",
	}

	assert.Equal(t, 0, int(td.Rows()))

	// Now insert the right way.
	td.InsertRow("foo", map[string]interface{}{
		"foo": "bar",
	})

	assert.Equal(t, 1, int(td.Rows()))
}

func TestTableData_InsertRowApproxSize(t *testing.T) {
	// In this test, we'll insert 1000 rows, update X and then delete Y
	// Does the size then match up? We will iterate over a map to take advantage of the in-deterministic ordering of a map
	// So we can test multiple updates, deletes, etc.
	td := NewTableData(nil, nil, kafkalib.TopicConfig{})
	numInsertRows := 1000
	numUpdateRows := 420
	numDeleteRows := 250

	for i := 0; i < numInsertRows; i ++ {
		td.InsertRow(fmt.Sprint(i), map[string]interface{}{
			"foo": "bar",
			"array": []int{1, 2, 3, 4, 5},
			"boolean": true,
			"nested_object": map[string]interface{}{
				"nested": map[string]interface{}{
					"foo": "bar",
					"true": false,
				},
			},
		})
	}

	var updateCount int
	for updateKey := range td.RowsData() {
		updateCount += 1
		td.InsertRow(updateKey, map[string]interface{}{
			"foo": "foo",
			"bar": "bar",
		})

		if updateCount > numUpdateRows {
			break
		}
	}

	var deleteCount int
	for deleteKey := range td.RowsData() {
		deleteCount += 1
		td.InsertRow(deleteKey, map[string]interface{}{
			"__artie_deleted": true,
		})

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
