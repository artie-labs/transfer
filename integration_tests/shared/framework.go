package shared

import (
	"context"
	"fmt"
	"time"

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
	return &TestFramework{
		ctx:         ctx,
		dest:        dest,
		tableID:     dest.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName),
		topicConfig: topicConfig,
	}
}

func (tf *TestFramework) SetupColumns(additionalColumns map[string]typing.KindDetails) {
	cols := &columns.Columns{}
	colTypes := map[string]typing.KindDetails{
		"id":         typing.Integer,
		"name":       typing.String,
		"created_at": typing.TimestampTZ,
		"value":      typing.Float,
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
	return map[string]any{
		"id":         pkValue,
		"name":       fmt.Sprintf("test_name_%d", pkValue),
		"created_at": time.Now().Format(time.RFC3339Nano),
		"value":      float64(pkValue) * 1.5,
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
	rows, err := tf.dest.Query(fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id", tf.tableID.FullyQualifiedName()))
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
		if err := rows.Scan(&id, &name, &value); err != nil {
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
	}

	if rows.Next() {
		return fmt.Errorf("unexpected extra rows found")
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	return nil
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
