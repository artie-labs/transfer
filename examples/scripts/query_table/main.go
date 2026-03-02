package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/sql"
)

func main() {
	ctx := context.Background()
	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	dest, err := utils.LoadSQLDestination(ctx, settings.Config)
	if err != nil {
		logger.Fatal("Unable to load data warehouse destination", slog.Any("err", err))
	}

	tc := settings.Config.TopicConfigs()
	if len(tc) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("received", len(tc)))
	}

	tableID := dest.IdentifierFor(tc[0].BuildDatabaseAndSchemaPair(), tc[0].TableName)

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 50", tableID.FullyQualifiedName())
	slog.Info("Running query", slog.String("query", query))
	rows, err := dest.QueryContext(ctx, query)
	if err != nil {
		logger.Fatal("Failed to query", slog.Any("err", err))
	}

	objects, err := sql.RowsToObjects(rows)
	if err != nil {
		logger.Fatal("Failed to convert rows to objects", slog.Any("err", err))
	}

	for _, object := range objects {
		fmt.Println(object)
	}
}
