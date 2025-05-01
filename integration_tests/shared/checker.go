package shared

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func (tf *TestFramework) scanAndCheckRow(rows *sql.Rows, i int) error {
	var id int
	var name string
	var value float64
	var jsonDataStr string
	var jsonArrayStr string
	var jsonStringStr string
	var jsonBooleanStr bool
	var jsonNumber int

	if tf.BigQuery() {
		// BigQuery does not support booleans, numbers and strings in a JSON column.
		if err := rows.Scan(&id, &name, &value, &jsonDataStr, &jsonArrayStr); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	} else {
		if err := rows.Scan(&id, &name, &value, &jsonDataStr, &jsonArrayStr, &jsonStringStr, &jsonBooleanStr, &jsonNumber); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	expectedName := fmt.Sprintf("test_name_%d", i)
	expectedValue := float64(i) * 1.5
	if id != i {
		return fmt.Errorf("unexpected id: expected %d, got %d", i, id)
	}
	if name != expectedName {
		return fmt.Errorf("unexpected name: expected %s, got %s", expectedName, name)
	}
	if value != expectedValue {
		return fmt.Errorf("unexpected value: expected %f, got %f", expectedValue, value)
	}

	// Verify JSON data
	expectedJSONData := map[string]interface{}{
		"field1": fmt.Sprintf("value_%d", i),
		"field2": i,
		"field3": i%2 == 0,
	}
	var actualJSONData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonDataStr), &actualJSONData); err != nil {
		return fmt.Errorf("failed to unmarshal json_data for row %d: %w", i, err)
	}

	// Normalize numeric types in actual JSON data
	if field2, ok := actualJSONData["field2"].(float64); ok {
		actualJSONData["field2"] = int(field2)
	}

	if !reflect.DeepEqual(expectedJSONData, actualJSONData) {
		return fmt.Errorf("unexpected json_data for row %d: expected %v, got %v", i, expectedJSONData, actualJSONData)
	}

	// Verify JSON array
	expectedJSONArray := []interface{}{
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_1", i),
			"array_field2": i + 1,
		},
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_2", i),
			"array_field2": i + 2,
		},
	}

	if ArrayAsListOfString(tf.dest) {
		expectedJSONArray = []any{
			fmt.Sprintf(`{"array_field1":"array_value_%d_1","array_field2":%d}`, i, i+1),
			fmt.Sprintf(`{"array_field1":"array_value_%d_2","array_field2":%d}`, i, i+2),
		}
	}

	var actualJSONArray []interface{}
	if err := json.Unmarshal([]byte(jsonArrayStr), &actualJSONArray); err != nil {
		return fmt.Errorf("failed to unmarshal json_array for row %d: %w", i, err)
	}

	// Normalize numeric types in actual JSON array
	for _, item := range actualJSONArray {
		if obj, ok := item.(map[string]interface{}); ok {
			if field2, ok := obj["array_field2"].(float64); ok {
				obj["array_field2"] = int(field2)
			}
		}
	}

	if !reflect.DeepEqual(expectedJSONArray, actualJSONArray) {
		return fmt.Errorf("unexpected json_array for row %d: expected %v, got %v", i, expectedJSONArray, actualJSONArray)
	}

	// Early exit if BigQuery
	if tf.BigQuery() {
		return nil
	}

	// Validate JSON string
	_dialect := tf.dest.Dialect()
	kd, err := _dialect.KindForDataType(_dialect.DataTypeForKind(typing.Struct, false, config.SharedDestinationColumnSettings{}), "")
	if err != nil {
		return fmt.Errorf("failed to get kind for data type: %w", err)
	}

	// Validate JSON boolean
	if jsonBooleanStr != (i%2 == 0) {
		return fmt.Errorf("unexpected json_boolean for row %d: expected %t, got %t", i, i%2 == 0, jsonBooleanStr)
	}

	// Validate JSON number
	if jsonNumber != i {
		return fmt.Errorf("unexpected json_number for row %d: expected %d, got %d", i, i, jsonNumber)
	}

	switch kd {
	case typing.String:
		if jsonStringStr != fmt.Sprintf("hello world %d", i) {
			return fmt.Errorf("unexpected json_string for row %d: expected %s, got %q", i, fmt.Sprintf("hello world %d", i), jsonStringStr)
		}
	case typing.Struct:
		if jsonStringStr != fmt.Sprintf(`"hello world %d"`, i) {
			return fmt.Errorf("unexpected json_string for row %d: expected %s, got %q", i, fmt.Sprintf(`"hello world %d"`, i), jsonStringStr)
		}
	default:
		return fmt.Errorf("unexpected data type: %q", kd.Kind)
	}

	return nil
}
