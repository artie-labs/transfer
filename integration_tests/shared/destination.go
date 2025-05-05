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

func (tf *TestFramework) verifyRowCountIceberg(expected int) error {
	resp, err := tf.iceberg.GetApacheLivyClient().QueryContext(tf.ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tf.tableID.FullyQualifiedName()))
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	if count := len(resp.Output.Data); count != expected {
		return fmt.Errorf("unexpected row count: expected %d, got %d", expected, count)
	}

	return nil
}
