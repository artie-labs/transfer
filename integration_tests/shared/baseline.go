package shared

import "fmt"

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

func (tf *TestFramework) verifyDataContentIceberg(rowCount int) error {
	// TODO
	return nil
}
