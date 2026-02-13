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

func NewAppendTest(ctx context.Context, dest destination.SQLDestination, _iceberg *iceberg.Store, topicConfig kafkalib.TopicConfig) *AppendTest {
	return &AppendTest{
		framework: shared.NewTestFramework(dest, _iceberg, topicConfig),
	}
}

func (at *AppendTest) generateTestData(ctx context.Context, numRows, appendEvery int) error {
	for i := 0; i < appendEvery; i++ {
		for j := 0; j < numRows; j++ {
			pkValue := i*numRows + j
			pkValueString := fmt.Sprintf("%d", pkValue)
			rowData := at.framework.GenerateRowData(pkValue)
			at.framework.GetTableData().InsertRow(pkValueString, rowData, false)
		}

		if err := at.framework.GetBaseline().Append(ctx, at.framework.GetTableData(), nil, false); err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}

		at.framework.GetTableData().WipeData()
	}

	return nil
}

func (at *AppendTest) Run(ctx context.Context) error {
	if err := at.framework.Cleanup(ctx, at.framework.GetTableID()); err != nil {
		return fmt.Errorf("failed to cleanup table: %w", err)
	}

	at.framework.SetupColumns(nil)

	appendRows := 200
	appendEvery := 50
	if err := at.generateTestData(ctx, appendRows, appendEvery); err != nil {
		return fmt.Errorf("failed to generate test data: %w", err)
	}

	if err := at.framework.VerifyRowCount(ctx, appendRows*appendEvery); err != nil {
		return fmt.Errorf("failed to verify row count: %w", err)
	}

	if err := at.framework.VerifyDataContent(ctx, appendRows*appendEvery); err != nil {
		return fmt.Errorf("failed to verify data content: %w", err)
	}

	return at.framework.Cleanup(ctx, at.framework.GetTableID())
}

func main() {
	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to load settings", slog.Any("err", err))
	}

	var _iceberg *iceberg.Store
	var dest destination.SQLDestination
	if settings.Config.Output == constants.Iceberg {
		loaded, err := utils.Load(ctx, settings.Config)
		if err != nil {
			logger.Fatal("Failed to load destination", slog.Any("err", err))
		}

		_icebergStore, ok := loaded.(iceberg.Store)
		if !ok {
			logger.Fatal(fmt.Sprintf("destination is not an iceberg store: %T", loaded))
		}

		_iceberg = &_icebergStore
	} else {
		dest, err = utils.LoadSQLDestination(ctx, settings.Config, nil)
		if err != nil {
			logger.Fatal("Failed to load destination", slog.Any("err", err))
		}
	}

	tcs := settings.Config.TopicConfigs()
	if len(tcs) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("num_configs", len(tcs)))
	}

	test := NewAppendTest(ctx, dest, _iceberg, *tcs[0])
	if err = test.Run(ctx); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info(fmt.Sprintf("🐕 🐕 🐕 Integration test for %q for append completed successfully", settings.Config.Output))
}
