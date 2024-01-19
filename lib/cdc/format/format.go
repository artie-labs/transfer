package format

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/lib/cdc/mysql"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/postgres"
	"github.com/artie-labs/transfer/lib/logger"
)

var (
	d     postgres.Debezium
	m     mongo.Debezium
	mySQL mysql.Debezium
)

func GetFormatParser(ctx context.Context, label, topic string) cdc.Format {
	validFormats := []cdc.Format{
		&d, &m, &mySQL,
	}

	for _, validFormat := range validFormats {
		for _, fmtLabel := range validFormat.Labels() {
			if fmtLabel == label {
				slog.Info("Loaded CDC Format parser...",
					slog.String("label", label),
					slog.String("topic", topic),
				)
				return validFormat
			}
		}
	}

	logger.Fatal("Failed to fetch CDC format parser", slog.String("label", label))
	return nil
}
