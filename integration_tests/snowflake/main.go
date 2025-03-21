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

func (t *SnowflakeTest) setupColumns() {
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

	t.tableData = optimization.NewTableData(cols, config.Replication, []string{"id"}, t.topicConfig, t.tableID.Table())
}

func (t *SnowflakeTest) generateTestData(numRows int) {
	for i := 0; i < numRows; i++ {
		rowData := map[string]any{
			"id":         i,
			"name":       fmt.Sprintf("test_name_%d", i),
			"created_at": time.Now().Format(time.RFC3339Nano),
			"value":      float64(i) * 1.5,
		}
		t.tableData.InsertRow(fmt.Sprintf("%d", i), rowData, false)
	}
}

func (t *SnowflakeTest) setupTable() error {
	if err := t.dest.DropTable(t.ctx, t.tableID); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := t.dest.Append(t.ctx, t.tableData, true); err != nil {
		return fmt.Errorf("failed to append data: %w", err)
	}

	return nil
}

func (t *SnowflakeTest) verifyRowCount(expected int) error {
	rows, err := t.dest.Query("SELECT COUNT(*) FROM test_db.public.test_table")
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

func (t *SnowflakeTest) verifyDataContent() error {
	rows, err := t.dest.Query("SELECT id, name, value FROM test_db.public.test_table ORDER BY id")
	if err != nil {
		return fmt.Errorf("failed to query table data: %w", err)
	}

	for i := 0; i < 120; i++ {
		if !rows.Next() {
			return fmt.Errorf("expected more rows: expected 120, got %d", i)
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

func (t *SnowflakeTest) cleanup() error {
	return t.dest.DropTable(t.ctx, t.tableID)
}

func (t *SnowflakeTest) Run() error {
	t.setupColumns()
	t.generateTestData(120)

	if err := t.setupTable(); err != nil {
		return err
	}

	if err := t.verifyRowCount(120); err != nil {
		return err
	}

	if err := t.verifyDataContent(); err != nil {
		return err
	}

	return t.cleanup()
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

	test := NewSnowflakeTest(ctx, dest, tc[0])
	if err := test.Run(); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info("Integration test completed successfully")
}
