package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	mssqlDialect "github.com/artie-labs/transfer/clients/mssql/dialect"
	"github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/integration_tests/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type DestinationTypes struct {
	destination destination.Destination
	topicConfig kafkalib.TopicConfig

	// Generated:
	tableID sql.TableIdentifier
}

func NewDestinationTypes(dest destination.Destination, topicConfig kafkalib.TopicConfig) (DestinationTypes, error) {
	return DestinationTypes{
		destination: dest,
		topicConfig: topicConfig,
		tableID:     dest.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), fmt.Sprintf("test_%s", stringutil.Random(10))),
	}, nil
}

func (d DestinationTypes) Run(ctx context.Context) error {
	defer func() {
		tableID := d.tableID.WithDisableDropProtection(true)
		if err := d.destination.DropTable(ctx, tableID); err != nil {
			logger.Fatal("Failed to drop table", slog.Any("err", err))
		}
	}()

	if _, ok := d.destination.Dialect().(dialect.RedshiftDialect); ok {
		if err := shared.RedshiftCreateTable(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		if err := shared.RedshiftAssertColumns(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to assert columns: %w", err)
		}

		return nil
	}

	if _, ok := d.destination.Dialect().(mssqlDialect.MSSQLDialect); ok {
		if err := shared.MSSQLCreateTable(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		if err := shared.MSSQLAssertColumns(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to assert columns: %w", err)
		}

		return nil
	}

	if _, ok := d.destination.Dialect().(snowflakeDialect.SnowflakeDialect); ok {
		if err := shared.SnowflakeCreateTable(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		if err := shared.SnowflakeAssertColumns(ctx, d.destination, d.tableID); err != nil {
			return fmt.Errorf("failed to assert columns: %w", err)
		}

		return nil
	}

	return fmt.Errorf("unsupported destination dialect: %T", d.destination.Dialect())
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

	tcs, err := settings.Config.TopicConfigs()
	if err != nil {
		logger.Fatal("Failed to get topic config", slog.Any("err", err))
	}

	if len(tcs) != 1 {
		logger.Fatal("Expected 1 topic config, got", slog.Any("count", len(tcs)))
	}

	destinationTypes, err := NewDestinationTypes(dest, *tcs[0])
	if err != nil {
		logger.Fatal("Failed to create destination types", slog.Any("err", err))
	}

	if err := destinationTypes.Run(ctx); err != nil {
		logger.Fatal("Failed to run destination types", slog.Any("err", err))
	}

	slog.Info(fmt.Sprintf("✌️ ✌️ ✌️ Destination types test completed successfully for: %T", dest.Dialect()))
}
