package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/logger"
)

var (
	primaryKeys           = []string{}
	includeArtieUpdatedAt *bool
)

func main() {
	if len(primaryKeys) == 0 {
		logger.Fatal("No primary keys provided")
	}

	if includeArtieUpdatedAt == nil {
		logger.Fatal("No includeArtieUpdatedAt provided")
	}

	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	dest, err := utils.LoadSQLDestination(ctx, settings.Config)
	if err != nil {
		logger.Fatal("Unable to load data warehouse destination", slog.Any("err", err))
	}

	tcs := settings.Config.TopicConfigs()
	if len(tcs) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("received", len(tcs)))
	}

	tableIdentifier := dest.IdentifierFor(tcs[0].BuildDatabaseAndSchemaPair(), tcs[0].TableName)
	slog.Info("Running Dedupe", slog.Any("tableIdentifier", tableIdentifier))
	if err := dest.Dedupe(ctx, tableIdentifier, tcs[0].BuildDatabaseAndSchemaPair(), primaryKeys, *includeArtieUpdatedAt); err != nil {
		logger.Fatal("Failed to dedupe", slog.Any("err", err))
	}

	slog.Info("Deduped")
}
