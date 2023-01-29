package typing

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBigQueryTypeToKind(t *testing.T) {
	bqColToExpectedKind := map[string]Kind{
		"string":             String,
		"sTriNG":             String,
		"STRING (10)":        String,
		"STRUCT<foo STRING>": Struct,
		"array<string>":      Array,
		"bool":               Boolean,
		"record":             Struct,
		"json":               Struct,
		"datetime":           DateTime,
		"timestamp":          DateTime,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		assert.Equal(t, BigQueryTypeToKind(bqCol), expectedKind, fmt.Sprintf("bqCol: %s did not match", bqCol))
	}
}
