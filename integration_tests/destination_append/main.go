package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/clients/iceberg"
	"github.com/artie-labs/transfer/integration_tests/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
)

type AppendTest struct {
	framework *shared.TestFramework
}

func NewAppendTest(ctx context.Context, dest destination.Destination, _iceberg *iceberg.Store, topicConfig kafkalib.TopicConfig) *AppendTest {
	return &AppendTest{
		framework: shared.NewTestFramework(ctx, dest, _iceberg, topicConfig),
	}
}

func (at *AppendTest) generateTestData(numRows int, appendEvery int) error {
	for i := 0; i < appendEvery; i++ {
		for j := 0; j < numRows; j++ {
			pkValue := i*numRows + j
			pkValueString := fmt.Sprintf("%d", pkValue)
			rowData := at.framework.GenerateRowData(pkValue)
			at.framework.GetTableData().InsertRow(pkValueString, rowData, false)
		}

		if err := at.framework.GetBaseline().Append(at.framework.GetContext(), at.framework.GetTableData(), false); err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}

		at.framework.GetTableData().WipeData()
	}

	return nil
}

func (at *AppendTest) Run() error {
	if err := at.framework.Cleanup(at.framework.GetTableID()); err != nil {
		return fmt.Errorf("failed to cleanup table: %w", err)
	}

	at.framework.SetupColumns(nil)

	appendRows := 200
	appendEvery := 2
	if err := at.generateTestData(appendRows, appendEvery); err != nil {
		return fmt.Errorf("failed to generate test data: %w", err)
	}

	if err := at.framework.VerifyRowCount(appendRows * appendEvery); err != nil {
		return fmt.Errorf("failed to verify row count: %w", err)
	}

	if err := at.framework.VerifyDataContent(appendRows * appendEvery); err != nil {
		return fmt.Errorf("failed to verify data content: %w", err)
	}

	return at.framework.Cleanup(at.framework.GetTableID())
}

func main() {
	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to load settings", slog.Any("err", err))
	}

	var _iceberg *iceberg.Store
	var dest destination.Destination
	if settings.Config.Output == constants.Iceberg {
		baseline, err := utils.LoadBaseline(ctx, settings.Config)
		if err != nil {
			logger.Fatal("Failed to load baseline", slog.Any("err", err))
		}

		_icebergStore, ok := baseline.(iceberg.Store)
		if !ok {
			logger.Fatal(fmt.Sprintf("baseline is not an iceberg store: %T", baseline))
		}

		_iceberg = &_icebergStore
	} else {
		dest, err = utils.LoadDestination(ctx, settings.Config, nil)
		if err != nil {
			logger.Fatal("Failed to load destination", slog.Any("err", err))
		}
	}

	tc, err := settings.Config.TopicConfigs()
	if err != nil {
		logger.Fatal("Failed to load topic configs", slog.Any("err", err))
	}

	if len(tc) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("num_configs", len(tc)))
	}

	test := NewAppendTest(ctx, dest, _iceberg, *tc[0])
	if err = test.Run(); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info(fmt.Sprintf("🐕 🐕 🐕 Integration test for %q for append completed successfully", settings.Config.Output))
}
