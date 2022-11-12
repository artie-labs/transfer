package format

import (
	"context"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/postgres"
	"github.com/artie-labs/transfer/lib/logger"
)

func GetFormatParser(ctx context.Context, label string) cdc.Format {
	var d postgres.Debezium
	if d.Label() == label {
		logger.FromContext(ctx).WithField("label", d.Label()).
			Info("Loaded CDC Format parser...")
		return &d
	}

	logger.FromContext(ctx).Fatalf("Failed to fetch")
	return nil
}
