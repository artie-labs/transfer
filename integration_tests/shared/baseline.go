package shared

import (
	"fmt"
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

	fmt.Println(string(bytes))

	// var getSchemaResp apachelivy.GetSchemaResponse
	// if err := json.Unmarshal(bytes, &getSchemaResp); err != nil {
	// 	return fmt.Errorf("failed to unmarshal response: %w", err)
	// }

	// count, err := strconv.Atoi(getSchemaResp.Data[0][0])
	// if err != nil {
	// 	return fmt.Errorf("failed to convert count to int: %w", err)
	// }

	// if count != expected {
	// 	return fmt.Errorf("unexpected row count: expected %d, got %d", expected, count)
	// }

	return nil
}

func (tf *TestFramework) verifyDataContentIceberg(rowCount int) error {
	// rows, err := tf.iceberg.GetApacheLivyClient().QueryContext(tf.ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tf.tableID.FullyQualifiedName()))
	// if err != nil {
	// 	return fmt.Errorf("failed to query table: %w", err)
	// }

	// TODO
	return nil
}
