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

func LoadExporter(ctx context.Context, kind config.ExporterKind, settings map[string]interface{}) context.Context {
	// TODO: support settings
	if !exporterKindValid(kind) {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"exporterKind": kind,
		}).Info("invalid or no exporter kind passed in, skipping...")
	}

	switch kind {
	case config.Datadog:
		var exportErr error
		ctx, exportErr = NewDatadogClient(ctx, settings)
		if exportErr != nil {
			logger.FromContext(ctx).WithField("provider", kind).Error(exportErr)
		} else {
			logger.FromContext(ctx).WithField("provider", kind).Info("Metrics client loaded")
		}
	}

	return ctx
}
