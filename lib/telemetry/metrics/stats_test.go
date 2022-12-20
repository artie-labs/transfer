package metrics

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
)

func TestExporterKindValid(t *testing.T) {
	exporterKindToResultsMap := map[config.ExporterKind]bool{
		config.Datadog:                      true,
		config.ExporterKind("daaaa"):        false,
		config.ExporterKind("daaaa231321"):  false,
		config.ExporterKind("honeycomb.io"): false,
	}

	for exporterKind, expectedResults := range exporterKindToResultsMap {
		assert.Equal(t, expectedResults, exporterKindValid(exporterKind),
			fmt.Sprintf("kind: %v should have been %v", exporterKind, expectedResults))
	}
}

func TestLoadExporter(t *testing.T) {
	// Datadog should not be a NullMetricsProvider
	exporterKindToResultMap := map[config.ExporterKind]bool{
		config.Datadog:                 false,
		config.ExporterKind("invalid"): true,
	}

	for kind, result := range exporterKindToResultMap {
		// Wipe and create a new ctx per run
		ctx := context.Background()
		ctx = LoadExporter(ctx, kind, map[string]interface{}{
			"url": "localhost:8125",
		})

		_, isOk := FromContext(ctx).(NullMetricsProvider)
		assert.Equal(t, result, isOk)
	}
}
