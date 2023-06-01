package format

import (
	"context"

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
				logger.FromContext(ctx).WithFields(map[string]interface{}{
					"label": label,
					"topic": topic,
				}).Info("Loaded CDC Format parser...")
				return validFormat
			}
		}
	}

	logger.FromContext(ctx).WithField("label", label).
		Fatalf("Failed to fetch CDC format parser")
	return nil
}
