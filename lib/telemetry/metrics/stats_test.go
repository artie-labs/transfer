package metrics

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func (m *MetricsTestSuite) TestExporterKindValid() {
	exporterKindToResultsMap := map[constants.ExporterKind]bool{
		constants.Datadog:                      true,
		constants.ExporterKind("daaaa"):        false,
		constants.ExporterKind("daaaa231321"):  false,
		constants.ExporterKind("honeycomb.io"): false,
	}

	for exporterKind, expectedResults := range exporterKindToResultsMap {
		assert.Equal(m.T(), expectedResults, exporterKindValid(exporterKind),
			fmt.Sprintf("kind: %v should have been %v", exporterKind, expectedResults))
	}
}

func (m *MetricsTestSuite) TestLoadExporter() {
	// Datadog should not be a NullMetricsProvider
	exporterKindToResultMap := map[constants.ExporterKind]bool{
		constants.Datadog:                 false,
		constants.ExporterKind("invalid"): true,
	}

	for kind, result := range exporterKindToResultMap {
		// Wipe and create a new ctx per run
		m.ctx = context.Background()
		cfg := config.Config{
			Telemetry: struct {
				Metrics struct {
					Provider constants.ExporterKind `yaml:"provider"`
					Settings map[string]interface{} `yaml:"settings,omitempty"`
				}
			}{
				Metrics: struct {
					Provider constants.ExporterKind `yaml:"provider"`
					Settings map[string]interface{} `yaml:"settings,omitempty"`
				}{
					Provider: kind,
					Settings: map[string]interface{}{
						"url": "localhost:8125",
					},
				},
			},
		}

		m.ctx = LoadExporter(m.ctx, cfg)
		_, isOk := FromContext(m.ctx).(NullMetricsProvider)
		assert.Equal(m.T(), result, isOk)
	}
}
