package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type SnowflakeTest struct {
	ctx         context.Context
	dest        destination.Destination
	tableID     dialect.TableIdentifier
	tableData   *optimization.TableData
	topicConfig kafkalib.TopicConfig
}

func NewSnowflakeTest(ctx context.Context, dest destination.Destination, topicConfig kafkalib.TopicConfig) *SnowflakeTest {
	return &SnowflakeTest{
		ctx:         ctx,
		dest:        dest,
		tableID:     dialect.NewTableIdentifier(topicConfig.Database, topicConfig.Schema, topicConfig.TableName),
		topicConfig: topicConfig,
	}
}

func (st *SnowflakeTest) setupColumns() {
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

	st.tableData = optimization.NewTableData(cols, config.Replication, []string{"id"}, st.topicConfig, st.tableID.Table())
}

func (st *SnowflakeTest) generateTestData(numRows int, appendEvery int) error {
	for i := 0; i < appendEvery; i++ {
		for j := 0; j < numRows; j++ {
			pkValue := i*numRows + j
			pkValueString := fmt.Sprintf("%d", pkValue)
			rowData := map[string]any{
				"id":         pkValue,
				"name":       fmt.Sprintf("test_name_%d", pkValue),
				"created_at": time.Now().Format(time.RFC3339Nano),
				"value":      float64(pkValue) * 1.5,
			}
			st.tableData.InsertRow(pkValueString, rowData, false)
		}

		if err := st.dest.Append(st.ctx, st.tableData, true); err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}

		st.tableData.WipeData()
	}

	return nil
}

func (st *SnowflakeTest) verifyRowCount(expected int) error {
	rows, err := st.dest.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", st.tableID.FullyQualifiedName()))
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to scan count: %w", err)
		}
	}

	if count != expected {
		return fmt.Errorf("unexpected row count: expected %d, got %d", expected, count)
	}

	return nil
}

func (st *SnowflakeTest) verifyDataContent(rowCount int) error {
	rows, err := st.dest.Query(fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id", st.tableID.FullyQualifiedName()))
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

	return nil
}

func (st *SnowflakeTest) cleanup(tableID dialect.TableIdentifier) error {
	dropTableID := tableID.WithDisableDropProtection(true)
	return st.dest.DropTable(st.ctx, dropTableID)
}

func (st *SnowflakeTest) Run() error {
	if err := st.cleanup(st.tableID); err != nil {
		return fmt.Errorf("failed to cleanup table: %w", err)
	}

	st.setupColumns()

	appendRows := 200
	appendEvery := 50
	if err := st.generateTestData(appendRows, appendEvery); err != nil {
		return fmt.Errorf("failed to generate test data: %w", err)
	}

	if err := st.verifyRowCount(appendRows * appendEvery); err != nil {
		return fmt.Errorf("failed to verify row count: %w", err)
	}

	if err := st.verifyDataContent(appendRows * appendEvery); err != nil {
		return fmt.Errorf("failed to verify data content: %w", err)
	}

	return st.cleanup(st.tableID)
}

func main() {
	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to load settings", slog.Any("err", err))
	}

	dest, err := utils.LoadDestination(ctx, settings.Config, nil)
	if err != nil {
		logger.Fatal("Failed to load destination", slog.Any("err", err))
	}

	tc, err := settings.Config.TopicConfigs()
	if err != nil {
		logger.Fatal("Failed to load topic configs", slog.Any("err", err))
	}

	if len(tc) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("num_configs", len(tc)))
	}

	test := NewSnowflakeTest(ctx, dest, *tc[0])
	if err := test.Run(); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info("🐕 🐕 🐕 Integration test completed successfully")
}
