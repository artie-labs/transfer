package format

import (
	"log/slog"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/eventtracking"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/relational"
	"github.com/artie-labs/transfer/lib/logger"
)

func GetFormatParser(label, topic string) cdc.Format {
	for _, validFormat := range []cdc.Format{relational.Debezium{}, mongo.Debezium{}, eventtracking.Format{}} {
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

	logger.Panic("Failed to fetch CDC format parser", slog.String("label", label))
	return nil
}
