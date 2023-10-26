package metrics

import (
	"context"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/datadog"

	"github.com/artie-labs/transfer/lib/logger"
)

var supportedExporterKinds = []constants.ExporterKind{constants.Datadog}

func exporterKindValid(kind constants.ExporterKind) bool {
	var valid bool
	for _, supportedExporterKind := range supportedExporterKinds {
		valid = kind == supportedExporterKind
		if valid {
			break
		}
	}

	return valid
}

func LoadExporter(ctx context.Context) context.Context {
	settings := config.FromContext(ctx)
	kind := settings.Config.Telemetry.Metrics.Provider
	ddSettings := settings.Config.Telemetry.Metrics.Settings

	if !exporterKindValid(kind) {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"exporterKind": kind,
		}).Info("invalid or no exporter kind passed in, skipping...")
	}

	switch kind {
	case constants.Datadog:
		var exportErr error
		ctx, exportErr = datadog.NewDatadogClient(ctx, ddSettings)
		if exportErr != nil {
			logger.FromContext(ctx).WithField("provider", kind).Error(exportErr)
		} else {
			logger.FromContext(ctx).WithField("provider", kind).Info("Metrics client loaded")
		}
	}

	return ctx
}
