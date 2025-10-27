package metrics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
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
		cfg := config.Config{
			Telemetry: struct {
				Metrics struct {
					Provider constants.ExporterKind `yaml:"provider"`
					Settings map[string]any         `yaml:"settings,omitempty"`
				}
			}{
				Metrics: struct {
					Provider constants.ExporterKind `yaml:"provider"`
					Settings map[string]any         `yaml:"settings,omitempty"`
				}{
					Provider: kind,
					Settings: map[string]any{
						"url": "localhost:8125",
					},
				},
			},
		}

		client := LoadExporter(cfg)
		_, ok := client.(NullMetricsProvider)
		assert.Equal(t, result, ok)
	}
}
