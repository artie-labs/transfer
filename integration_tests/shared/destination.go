package shared

import "fmt"

func (tf *TestFramework) verifyRowCountDestination(expected int) error {
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

func (tf *TestFramework) verifyDataContentDestination(rowCount int) error {
	baseQuery := fmt.Sprintf("SELECT id, name, value, json_data, json_array, json_string, json_boolean, json_number FROM %s ORDER BY id", tf.tableID.FullyQualifiedName())

	if tf.BigQuery() {
		// BigQuery does not support booleans, numbers and strings in a JSON column.
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

		if err := tf.scanAndCheckRow(rows, i); err != nil {
			return fmt.Errorf("failed to check row %d: %w", i, err)
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
