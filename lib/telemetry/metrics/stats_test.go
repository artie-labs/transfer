package metrics

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExporterKindValid(t *testing.T) {
	exporterKindToResultsMap := map[constants.ExporterKind]bool{
		constants.Datadog:                      true,
		constants.ExporterKind("daaaa"):        false,
		constants.ExporterKind("daaaa231321"):  false,
		constants.ExporterKind("honeycomb.io"): false,
	}

	for exporterKind, expectedResults := range exporterKindToResultsMap {
		assert.Equal(t, expectedResults, exporterKindValid(exporterKind),
			fmt.Sprintf("kind: %v should have been %v", exporterKind, expectedResults))
	}
}

func TestLoadExporter(t *testing.T) {
	// Datadog should not be a NullMetricsProvider
	exporterKindToResultMap := map[constants.ExporterKind]bool{
		constants.Datadog:                 false,
		constants.ExporterKind("invalid"): true,
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
