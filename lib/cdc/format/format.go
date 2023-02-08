package format

import (
	"context"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/postgres"
	"github.com/artie-labs/transfer/lib/logger"
)

var (
	d postgres.Debezium
	m mongo.Debezium
)

func GetFormatParser(ctx context.Context, label string) cdc.Format {
	validFormats := []cdc.Format{
		&d, &m,
	}

	for _, validFormat := range validFormats {
		for _, fmtLabel := range validFormat.Labels() {
			if fmtLabel == label {
				logger.FromContext(ctx).WithField("label", fmtLabel).Info("Loaded CDC Format parser...")
				return validFormat
			}
		}
	}

	logger.FromContext(ctx).WithField("label", label).
		Fatalf("Failed to fetch CDC format parser")
	return nil
}
