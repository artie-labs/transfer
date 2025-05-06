package shared

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/apachelivy"
)

func (tf *TestFramework) verifyRowCountIceberg(expected int) error {
	resp, err := tf.iceberg.GetApacheLivyClient().QueryContext(tf.ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tf.tableID.FullyQualifiedName()))
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	var getSchemaResp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &getSchemaResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	count, ok := getSchemaResp.Data[0][0].(float64)
	if !ok {
		return fmt.Errorf("row count is not a float")
	}

	if int(count) != expected {
		return fmt.Errorf("unexpected row count: expected %d, got %d", expected, int(count))
	}

	return nil
}

func (tf *TestFramework) verifyDataContentIceberg(rowCount int) error {
	query := fmt.Sprintf("SELECT id, name, value, json_data, json_array, json_string, json_boolean, json_number FROM %s ORDER BY id", tf.tableID.FullyQualifiedName())
	resp, err := tf.iceberg.GetApacheLivyClient().QueryContext(tf.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	var getSchemaResp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &getSchemaResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	columns, err := getSchemaResp.BuildColumns()
	if err != nil {
		return fmt.Errorf("failed to build columns: %w", err)
	}

	for _, rowValues := range getSchemaResp.Data {
		row := make(map[string]any)
		for i, value := range rowValues {
			row[columns[i].Name] = value
		}

	}

	// TODO
	return nil
}
