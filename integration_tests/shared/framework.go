package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	databricksdialect "github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type TestFramework struct {
	ctx         context.Context
	dest        destination.Destination
	tableID     sql.TableIdentifier
	tableData   *optimization.TableData
	topicConfig kafkalib.TopicConfig
}

func NewTestFramework(ctx context.Context, dest destination.Destination, topicConfig kafkalib.TopicConfig) *TestFramework {
	cols := columns.NewColumns([]columns.Column{
		columns.NewColumn("id", typing.Integer),
		columns.NewColumn("name", typing.String),
		columns.NewColumn("value", typing.Float),
		columns.NewColumn("json_data", typing.Struct),
		columns.NewColumn("array_data", typing.Array),
	})

	return &TestFramework{
		ctx:         ctx,
		dest:        dest,
		tableID:     dest.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName),
		topicConfig: topicConfig,
		tableData:   optimization.NewTableData(cols, config.Replication, []string{"id"}, topicConfig, dest.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName).Table()),
	}
}

func (tf *TestFramework) SetupColumns(additionalColumns map[string]typing.KindDetails) {
	cols := &columns.Columns{}
	colTypes := map[string]typing.KindDetails{
		"id":         typing.Integer,
		"name":       typing.String,
		"created_at": typing.TimestampTZ,
		"value":      typing.Float,
		"json_data":  typing.Struct,
		"json_array": typing.Array,
	}

	for colName, colType := range colTypes {
		cols.AddColumn(columns.NewColumn(colName, colType))
	}

	for colName, colType := range additionalColumns {
		cols.AddColumn(columns.NewColumn(colName, colType))
	}

	tf.tableData = optimization.NewTableData(cols, config.Replication, []string{"id"}, tf.topicConfig, tf.tableID.Table())
}

func (tf *TestFramework) GenerateRowDataForMerge(pkValue int, delete bool) map[string]any {
	row := tf.GenerateRowData(pkValue)
	row[constants.DeleteColumnMarker] = delete
	row[constants.OnlySetDeleteColumnMarker] = delete
	return row
}

func (tf *TestFramework) GenerateRowData(pkValue int) map[string]any {
	jsonData := map[string]interface{}{
		"field1": fmt.Sprintf("value_%d", pkValue),
		"field2": pkValue,
		"field3": pkValue%2 == 0,
	}

	jsonArray := []interface{}{
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_1", pkValue),
			"array_field2": pkValue + 1,
		},
		map[string]interface{}{
			"array_field1": fmt.Sprintf("array_value_%d_2", pkValue),
			"array_field2": pkValue + 2,
		},
	}

	return map[string]any{
		"id":         pkValue,
		"name":       fmt.Sprintf("test_name_%d", pkValue),
		"created_at": time.Now().Format(time.RFC3339Nano),
		"value":      float64(pkValue) * 1.5,
		"json_data":  jsonData,
		"json_array": jsonArray,
	}
}

func (tf *TestFramework) VerifyRowCount(expected int) error {
	rows, err := tf.dest.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", tf.tableID.FullyQualifiedName()))
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to scan count: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	if count != expected {
		return fmt.Errorf("unexpected row count: expected %d, got %d", expected, count)
	}

	return nil
}

func (tf *TestFramework) VerifyDataContent(rowCount int) error {
	baseQuery := fmt.Sprintf("SELECT id, name, value, json_data, json_array FROM %s ORDER BY id", tf.tableID.FullyQualifiedName())

	if _, ok := tf.dest.Dialect().(dialect.BigQueryDialect); ok {
		baseQuery = fmt.Sprintf("SELECT id, name, value, TO_JSON_STRING(json_data), TO_JSON_STRING(json_array) FROM %s ORDER BY id", tf.tableID.FullyQualifiedName())
	}

	rows, err := tf.dest.Query(baseQuery)
	if err != nil {
		return fmt.Errorf("failed to query table data: %w", err)
	}

	for i := 0; i < rowCount; i++ {
		if !rows.Next() {
			return fmt.Errorf("expected more rows: expected %d, got %d", rowCount, i)
		}

		var id int
		var name string
		var value float64
		var jsonDataStr string
		var jsonArrayStr string
		if err := rows.Scan(&id, &name, &value, &jsonDataStr, &jsonArrayStr); err != nil {
			return fmt.Errorf("failed to scan row %d: %w", i, err)
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
	}

	if rows.Next() {
		return fmt.Errorf("unexpected extra rows found")
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	return nil
}

func (tf *TestFramework) VerifyDataContentWithRows(ctx context.Context, tableName string, rowCount int) error {
	rows, err := tf.dest.Query(fmt.Sprintf("SELECT id, name, value, json_data, json_array FROM %s ORDER BY id", tableName))
	if err != nil {
		return fmt.Errorf("failed to query table: %v", err)
	}
	defer rows.Close()

	for i := 0; i < rowCount; i++ {
		if !rows.Next() {
			return fmt.Errorf("expected %d rows but got %d", rowCount, i)
		}

		var id int
		var name string
		var value float64
		var jsonData []byte
		var arrayData []byte

		if err := rows.Scan(&id, &name, &value, &jsonData, &arrayData); err != nil {
			return fmt.Errorf("failed to scan row %d: %v", i, err)
		}

		// Verify id
		if id != i+1 {
			return fmt.Errorf("unexpected id value: got %d, want %d", id, i+1)
		}

		// Verify name
		expectedName := fmt.Sprintf("name_%d", i+1)
		if name != expectedName {
			return fmt.Errorf("unexpected name value: got %s, want %s", name, expectedName)
		}

		// Verify value
		expectedValue := float64(i+1) * 1.5
		if value != expectedValue {
			return fmt.Errorf("unexpected value: got %f, want %f", value, expectedValue)
		}

		// Verify JSON data
		var jsonMap map[string]interface{}
		if err := json.Unmarshal(jsonData, &jsonMap); err != nil {
			return fmt.Errorf("failed to unmarshal JSON data: %v", err)
		}

		if jsonMap["key"] != fmt.Sprintf("value_%d", i+1) {
			return fmt.Errorf("unexpected JSON value: got %v, want value_%d", jsonMap["key"], i+1)
		}

		// Verify array data
		var array []int
		if err := json.Unmarshal(arrayData, &array); err != nil {
			return fmt.Errorf("failed to unmarshal array data: %v", err)
		}

		expectedArray := []int{i + 1, i + 2, i + 3}
		if !reflect.DeepEqual(array, expectedArray) {
			return fmt.Errorf("unexpected array value: got %v, want %v", array, expectedArray)
		}
	}

	if rows.Next() {
		return fmt.Errorf("found extra rows beyond expected count of %d", rowCount)
	}

	return rows.Err()
}

func (tf *TestFramework) Cleanup(tableID sql.TableIdentifier) error {
	dropTableID := tableID.WithDisableDropProtection(true)
	return tf.dest.DropTable(tf.ctx, dropTableID)
}

func (tf *TestFramework) GetTableData() *optimization.TableData {
	return tf.tableData
}

func (tf *TestFramework) GetTableID() sql.TableIdentifier {
	return tf.tableID
}

func (tf *TestFramework) GetDestination() destination.Destination {
	return tf.dest
}

func (tf *TestFramework) GetContext() context.Context {
	return tf.ctx
}

// These destinations return array as array<string>.
func ArrayAsListOfString(dest destination.Destination) bool {
	switch dest.Dialect().(type) {
	case dialect.BigQueryDialect, databricksdialect.DatabricksDialect:
		return true
	default:
		return false
	}
}
