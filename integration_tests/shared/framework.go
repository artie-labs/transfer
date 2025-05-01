package shared

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	databricksdialect "github.com/artie-labs/transfer/clients/databricks/dialect"
	mssqlDialect "github.com/artie-labs/transfer/clients/mssql/dialect"
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

func (t TestFramework) BigQuery() bool {
	_, ok := t.dest.Dialect().(dialect.BigQueryDialect)
	return ok
}

func (t TestFramework) MSSQL() bool {
	_, ok := t.dest.Dialect().(mssqlDialect.MSSQLDialect)
	return ok
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
		"json_data":  typing.Struct,
		"json_array": typing.Array,
	}

	if !tf.BigQuery() {
		colTypes["json_string"] = typing.Struct
		colTypes["json_boolean"] = typing.Struct
		colTypes["json_number"] = typing.Struct
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

	row := map[string]any{
		"id":         pkValue,
		"name":       fmt.Sprintf("test_name_%d", pkValue),
		"created_at": time.Now().Format(time.RFC3339Nano),
		"value":      float64(pkValue) * 1.5,
		"json_data":  jsonData,
		"json_array": jsonArray,
	}

	if !tf.BigQuery() {
		row["json_string"] = fmt.Sprintf("hello world %d", pkValue)
		row["json_boolean"] = pkValue%2 == 0
		row["json_number"] = pkValue
	}

	return row
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
	baseQuery := fmt.Sprintf("SELECT id, name, value, json_data, json_array, json_string, json_boolean, json_number FROM %s ORDER BY id", tf.tableID.FullyQualifiedName())

	if _, ok := tf.dest.Dialect().(dialect.BigQueryDialect); ok {
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
