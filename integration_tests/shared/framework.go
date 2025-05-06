package shared

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	databricksdialect "github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/clients/iceberg"
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
	tableID     sql.TableIdentifier
	tableData   *optimization.TableData
	topicConfig kafkalib.TopicConfig

	dest    destination.Destination
	iceberg *iceberg.Store
}

func (t TestFramework) BigQuery() bool {
	if t.dest == nil {
		return false
	}

	_, ok := t.dest.Dialect().(dialect.BigQueryDialect)
	return ok
}

func (t TestFramework) MSSQL() bool {
	if t.dest == nil {
		return false
	}

	_, ok := t.dest.Dialect().(mssqlDialect.MSSQLDialect)
	return ok
}

func NewTestFramework(ctx context.Context, dest destination.Destination, _iceberg *iceberg.Store, topicConfig kafkalib.TopicConfig) *TestFramework {
	var tableID sql.TableIdentifier
	if _iceberg != nil {
		tableID = _iceberg.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName)
	} else {
		tableID = dest.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName)
	}

	return &TestFramework{
		ctx:         ctx,
		dest:        dest,
		iceberg:     _iceberg,
		tableID:     tableID,
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
	if tf.iceberg != nil {
		return tf.verifyRowCountIceberg(expected)
	}

	return tf.verifyRowCountDestination(expected)
}

func (tf *TestFramework) VerifyDataContent(rowCount int) error {
	if tf.iceberg != nil {
		return tf.verifyDataContentIceberg(rowCount)
	}

	return tf.verifyDataContentDestination(rowCount)
}

func (tf *TestFramework) Cleanup(tableID sql.TableIdentifier) error {
	dropTableID := tableID.WithDisableDropProtection(true)
	if tf.iceberg != nil {
		return tf.iceberg.DeleteTable(tf.ctx, dropTableID)
	}

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

func (tf *TestFramework) GetBaseline() destination.Baseline {
	if tf.iceberg != nil {
		return tf.iceberg
	}

	return tf.dest
}

func (tf *TestFramework) GetContext() context.Context {
	return tf.ctx
}

// These destinations return array as array<string>.
func (tf *TestFramework) ArrayAsListOfString() bool {
	if tf.iceberg != nil {
		return false
	}

	switch tf.dest.Dialect().(type) {
	case dialect.BigQueryDialect, databricksdialect.DatabricksDialect:
		return true
	default:
		return false
	}
}
