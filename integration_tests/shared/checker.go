package shared

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/artie-labs/transfer/lib/typing"
)

func (tf *TestFramework) scanAndCheckRow(rows *sql.Rows, i int) error {
	return tf.VerifyRowData(rows, i, 1.5)
}

// VerifyRowData verifies the data in a row matches the expected values
func (tf *TestFramework) VerifyRowData(rows *sql.Rows, i int, valueMultiplier float64) error {
	var id int
	var name string
	var value float64
	var jsonDataStr string
	var jsonArrayStr string
	var jsonStringStr string
	var jsonBoolean bool
	var jsonNumber string

	if tf.BigQuery() {
		// BigQuery does not support booleans, numbers and strings in a JSON column.
		if err := rows.Scan(&id, &name, &value, &jsonDataStr, &jsonArrayStr); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	} else {
		if err := rows.Scan(&id, &name, &value, &jsonDataStr, &jsonArrayStr, &jsonStringStr, &jsonBoolean, &jsonNumber); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	row := map[string]any{
		"id":           id,
		"name":         name,
		"value":        value,
		"json_data":    jsonDataStr,
		"json_array":   jsonArrayStr,
		"json_string":  jsonStringStr,
		"json_boolean": jsonBoolean,
		"json_number":  jsonNumber,
	}

	if err := tf.verifyRowData(row, i, valueMultiplier); err != nil {
		return fmt.Errorf("failed to verify row data: %w", err)
	}

	return nil
}

func (tf *TestFramework) verifyRowData(row map[string]any, index int, valueMultiplier float64) error {
	id, err := typing.AssertType[int](row["id"])
	if err != nil {
		return err
	}

	name, err := typing.AssertType[string](row["name"])
	if err != nil {
		return err
	}

	value, err := typing.AssertType[float64](row["value"])
	if err != nil {
		return err
	}

	jsonDataStr, err := typing.AssertType[string](row["json_data"])
	if err != nil {
		return err
	}

	jsonArrayStr, err := typing.AssertType[string](row["json_array"])
	if err != nil {
		return err
	}

	jsonStringStr, err := typing.AssertType[string](row["json_string"])
	if err != nil {
		return err
	}

	jsonBoolean, err := typing.AssertType[bool](row["json_boolean"])
	if err != nil {
		return err
	}

	jsonNumber, err := typing.AssertType[string](row["json_number"])
	if err != nil {
		return fmt.Errorf("json_number is not a string: %w", err)
	}

	expectedName := fmt.Sprintf("test_name_%d", index)
	expectedValue := float64(index) * valueMultiplier
	if id != index {
		return fmt.Errorf("unexpected id: expected %d, got %d", index, id)
	}
	if name != expectedName {
		return fmt.Errorf("unexpected name: expected %s, got %s", expectedName, name)
	}
	if value != expectedValue {
		return fmt.Errorf("unexpected value: expected %f, got %f", expectedValue, value)
	}

	// Verify JSON data
	expectedJSONData := map[string]interface{}{
		"field1": fmt.Sprintf("value_%d", index),
		"field2": index,
		"field3": index%2 == 0,
	}

	var actualJSONData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonDataStr), &actualJSONData); err != nil {
		return fmt.Errorf("failed to unmarshal json_data for row %d: %w", index, err)
	}

	// Normalize numeric types in actual JSON data
	if field2, ok := actualJSONData["field2"].(float64); ok {
		actualJSONData["field2"] = int(field2)
	}

	if !reflect.DeepEqual(expectedJSONData, actualJSONData) {
		return fmt.Errorf("unexpected json_data for row %d: expected %v, got %v", index, expectedJSONData, actualJSONData)
	}

	// Verify JSON array
	expectedJSONArray := []interface{}{
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_1", index),
			"array_field2": index + 1,
		},
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_2", index),
			"array_field2": index + 2,
		},
	}

	if tf.ArrayAsListOfString() {
		expectedJSONArray = []any{
			fmt.Sprintf(`{"array_field1":"array_value_%d_1","array_field2":%d}`, index, index+1),
			fmt.Sprintf(`{"array_field1":"array_value_%d_2","array_field2":%d}`, index, index+2),
		}
	}

	var actualJSONArray []interface{}
	if err := json.Unmarshal([]byte(jsonArrayStr), &actualJSONArray); err != nil {
		return fmt.Errorf("failed to unmarshal json_array for row %d: %w", index, err)
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

	// Validate JSON boolean
	if jsonBoolean != (index%2 == 0) {
		return fmt.Errorf("unexpected json_boolean for row %d: expected %t, got %t", index, index%2 == 0, jsonBoolean)
	}

	// Validate JSON number
	if jsonNumber != fmt.Sprintf("%d", index) {
		return fmt.Errorf("unexpected json_number for row %d: expected %s, got %q", index, fmt.Sprintf("%d", index), jsonNumber)
	}

	// Validate JSON string
	expectedJSONString := fmt.Sprintf(`"hello world %d"`, index)

	if tf.MSSQL() {
		expectedJSONString = fmt.Sprintf("hello world %d", index)
	}

	if jsonStringStr != expectedJSONString {
		return fmt.Errorf("unexpected json_string for row %d: expected %s, got %q", index, expectedJSONString, jsonStringStr)
	}
}
