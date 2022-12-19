package metrics

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/logger"
)

var supportedExporterKinds = []config.ExporterKind{config.Datadog}

func exporterKindValid(kind config.ExporterKind) bool {
	var valid bool
	for _, supportedExporterKind := range supportedExporterKinds {
		valid = kind == supportedExporterKind
		if valid {
			break
		}
	}

	return valid
}

func LoadExporter(ctx context.Context, kind config.ExporterKind, settings map[string]interface{}) error {
	// TODO: support settings
	if !exporterKindValid(kind) {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"exporterKind": kind,
		}).Info("invalid or no exporter kind passed in, skipping...")
		return nil
	}

	switch kind {
	case config.Datadog:
		return NewDatadogClient(ctx, settings)
	}

	return nil
}
