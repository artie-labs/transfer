package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/logger"
)

type DestinationTypes struct {
	destination destination.Destination
}

func (d DestinationTypes) Run() error {
	return nil
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

	destinationTypes := DestinationTypes{destination: dest}

	if err := destinationTypes.Run(); err != nil {
		logger.Fatal("Failed to run destination types", slog.Any("err", err))
	}
}
