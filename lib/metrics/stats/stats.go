package stats

import (
	"context"
	"github.com/artie-labs/transfer/lib/config"
	"log"

	datadog "github.com/DataDog/opencensus-go-exporter-datadog"
	"go.opencensus.io/stats/view"

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

func LoadExporter(ctx context.Context, kind config.ExporterKind, settings map[string]interface{}) {
	// TODO: support settings
	if !exporterKindValid(kind) {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"exporterKind": kind,
		}).Info("invalid or no exporter kind passed in, skipping...")
		return
	}

	switch kind {
	case config.Datadog:
		exporter, err := datadog.NewExporter(datadog.Options{})
		if err != nil {
			log.Fatal(err)
		}

		view.RegisterExporter(exporter)
	}
}
