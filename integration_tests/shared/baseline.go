package shared

import (
	"encoding/json"
	"fmt"
	"strconv"

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
	// Limit this to 1k rows since that's the default limit for Livy.
	iters := rowCount / 1000
	totalRows := 0
	for iter := 0; iter < iters; iter++ {
		offset := iter * 1000
		query := fmt.Sprintf("SELECT id, name, value, json_data, json_array, json_string, json_boolean, json_number FROM %s ORDER BY id LIMIT 1000 OFFSET %d", tf.tableID.FullyQualifiedName(), offset)
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

		var colNames []string
		for _, field := range getSchemaResp.Schema.Fields {
			colNames = append(colNames, string(field.Name))
		}

		for rowNumber, rowValues := range getSchemaResp.Data {
			row := make(map[string]any)
			for i, value := range rowValues {
				switch colNames[i] {
				case "json_boolean":
					castedValue, err := strconv.ParseBool(value.(string))
					if err != nil {
						return fmt.Errorf("failed to parse json_boolean: %w", err)
					}

					row[colNames[i]] = castedValue
				default:
					row[colNames[i]] = value
				}
			}

			if err := tf.verifyRowData(row, rowNumber+offset, 1.5); err != nil {
				return fmt.Errorf("failed to verify row %d: %w", rowNumber, err)
			}
		}

		totalRows += len(getSchemaResp.Data)
	}

	if totalRows != rowCount {
		return fmt.Errorf("unexpected row count: expected %d, got %d", rowCount, totalRows)
	}

	return nil
}
