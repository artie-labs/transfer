package metrics

import (
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/datadog"
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

func LoadExporter(cfg config.Config) base.Client {
	kind := cfg.Telemetry.Metrics.Provider
	ddSettings := cfg.Telemetry.Metrics.Settings
	if !exporterKindValid(kind) {
		slog.Info("Invalid or no exporter kind passed in, skipping...", slog.Any("exporterKind", kind))
	}

	switch kind {
	case constants.Datadog:
		statsClient, exportErr := datadog.NewDatadogClient(ddSettings)
		if exportErr != nil {
			slog.Error("Metrics client error", slog.Any("err", exportErr), slog.Any("provider", kind))
		} else {
			slog.Info("Metrics client loaded", slog.Any("provider", kind))
			return statsClient
		}
	}

	return NullMetricsProvider{}
}
