package metrics

import (
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
