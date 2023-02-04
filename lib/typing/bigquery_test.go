package typing

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBigQueryTypeToKind(t *testing.T) {
	bqColToExpectedKind := map[string]Kind{
		// Integer
		"int":     Integer,
		"integer": Integer,
		"inT64":   Integer,
		// String
		"varchar":     String,
		"string":      String,
		"sTriNG":      String,
		"STRING (10)": String,
		// Array
		"array<integer>": Array,
		"array<string>":  Array,
		// Boolean
		"bool":    Boolean,
		"boolean": Boolean,
		// Struct
		"STRUCT<foo STRING>": Struct,
		"record":             Struct,
		"json":               Struct,
		// Datetime
		"datetime":  DateTime,
		"timestamp": DateTime,
		"time":      DateTime,
		"date":      DateTime,
		//Invalid
		"foo":    Invalid,
		"foofoo": Invalid,
		"":       Invalid,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		assert.Equal(t, BigQueryTypeToKind(bqCol), expectedKind, fmt.Sprintf("bqCol: %s did not match", bqCol))
	}
}
