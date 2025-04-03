package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/clients/mssql/dialect"
	"github.com/artie-labs/transfer/integration_tests/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing"
)

type MergeTest struct {
	framework *shared.TestFramework
}

func NewMergeTest(ctx context.Context, dest destination.Destination, topicConfig kafkalib.TopicConfig) *MergeTest {
	return &MergeTest{
		framework: shared.NewTestFramework(ctx, dest, topicConfig),
	}
}

func (mt *MergeTest) generateInitialData(numRows int) error {
	for i := 0; i < numRows; i++ {
		pkValueString := fmt.Sprintf("%d", i)
		rowData := mt.framework.GenerateRowDataForMerge(i, false)
		mt.framework.GetTableData().InsertRow(pkValueString, rowData, false)
	}

	if _, err := mt.framework.GetDestination().Merge(mt.framework.GetContext(), mt.framework.GetTableData()); err != nil {
		return fmt.Errorf("failed to merge initial data: %w", err)
	}

	mt.framework.GetTableData().WipeData()
	return nil
}

func (mt *MergeTest) updateExistingData(numRows int) error {
	for i := 0; i < numRows; i++ {
		pkValueString := fmt.Sprintf("%d", i)
		rowData := mt.framework.GenerateRowDataForMerge(i, false)
		// Modify the value to indicate an update
		rowData["value"] = float64(i) * 2.0
		mt.framework.GetTableData().InsertRow(pkValueString, rowData, false)
	}

	if _, err := mt.framework.GetDestination().Merge(mt.framework.GetContext(), mt.framework.GetTableData()); err != nil {
		return fmt.Errorf("failed to merge updates: %w", err)
	}

	mt.framework.GetTableData().WipeData()
	return nil
}

func (mt *MergeTest) deleteData(numRows int) error {
	for i := 0; i < numRows; i++ {
		pkValueString := fmt.Sprintf("%d", i)
		rowData := mt.framework.GenerateRowDataForMerge(i, true)
		mt.framework.GetTableData().InsertRow(pkValueString, rowData, true)
	}

	if _, err := mt.framework.GetDestination().Merge(mt.framework.GetContext(), mt.framework.GetTableData()); err != nil {
		return fmt.Errorf("failed to merge deletes: %w", err)
	}

	mt.framework.GetTableData().WipeData()
	return nil
}

func (mt *MergeTest) verifyUpdatedData(numRows int) error {
	query := fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id ASC LIMIT %d", mt.framework.GetTableID().FullyQualifiedName(), numRows)
	if _, ok := mt.framework.GetDestination().Dialect().(dialect.MSSQLDialect); ok {
		query = fmt.Sprintf("SELECT TOP %d id, name, value FROM %s ORDER BY id ASC", numRows, mt.framework.GetTableID().FullyQualifiedName())
	}

	rows, err := mt.framework.GetDestination().Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table data: %w, query: %q", err, query)
	}

	for i := 0; i < numRows; i++ {
		if !rows.Next() {
			return fmt.Errorf("expected more rows: expected %d, got %d", numRows, i)
		}

		var id int
		var name string
		var value float64
		if err := rows.Scan(&id, &name, &value); err != nil {
			return fmt.Errorf("failed to scan row %d: %w", i, err)
		}

		expectedName := fmt.Sprintf("test_name_%d", i)
		expectedValue := float64(i) * 2.0 // Updated value
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

func (mt *MergeTest) Run() error {
	if err := mt.framework.Cleanup(mt.framework.GetTableID()); err != nil {
		return fmt.Errorf("failed to cleanup table: %w", err)
	}

	mt.framework.SetupColumns(map[string]typing.KindDetails{
		constants.DeleteColumnMarker:        typing.Boolean,
		constants.OnlySetDeleteColumnMarker: typing.Boolean,
	})

	numRows := 1000
	if err := mt.generateInitialData(numRows); err != nil {
		return fmt.Errorf("failed to generate initial data: %w", err)
	}

	if err := mt.framework.VerifyRowCount(numRows); err != nil {
		return fmt.Errorf("failed to verify initial row count: %w", err)
	}

	if err := mt.framework.VerifyDataContent(numRows); err != nil {
		return fmt.Errorf("failed to verify initial data content: %w", err)
	}

	// Update only 20% of the rows
	updatedRows := int(float64(numRows) * 0.2)
	if err := mt.updateExistingData(updatedRows); err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}

	if err := mt.verifyUpdatedData(updatedRows); err != nil {
		return fmt.Errorf("failed to verify updated data: %w", err)
	}

	// Delete only 20% of the rows
	rowsToDelete := int(float64(numRows) * 0.2)
	if err := mt.deleteData(rowsToDelete); err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	if err := mt.framework.VerifyRowCount(numRows - rowsToDelete); err != nil {
		return fmt.Errorf("failed to verify final row count: %w", err)
	}

	return mt.framework.Cleanup(mt.framework.GetTableID())
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

	test := NewMergeTest(ctx, dest, *tc[0])
	if err := test.Run(); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info(fmt.Sprintf("🐕 🐕 🐕 Integration test for %q for merge completed successfully", settings.Config.Output))
}
