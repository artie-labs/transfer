package format

import (
	"context"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/postgres"
	"github.com/artie-labs/transfer/lib/logger"
)

func GetFormatParser(ctx context.Context, label string) cdc.Format {
	var d postgres.Debezium
	var m mongo.Mongo

	if d.Label() == label {
		logger.FromContext(ctx).WithField("label", d.Label()).
			Info("Loaded CDC Format parser...")
		return &d
	}

	if m.Label() == label {
		logger.FromContext(ctx).WithField("label", m.Label()).
			Info("Loaded CDC Format parser...")
		return &m
	}

	logger.FromContext(ctx).WithField("label", label).
		Fatalf("Failed to fetch CDC format parser")
	return nil
}
