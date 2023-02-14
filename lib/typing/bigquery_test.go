package typing

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBigQueryTypeToKind(t *testing.T) {
	bqColToExpectedKind := map[string]KindDetails{
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
		"datetime":  NewKindDetailsFromTemplate(ETime, DateTimeKindType),
		"timestamp": NewKindDetailsFromTemplate(ETime, DateTimeKindType),
		"time":      NewKindDetailsFromTemplate(ETime, TimeKindType),
		"date":      NewKindDetailsFromTemplate(ETime, DateKindType),
		//Invalid
		"foo":    Invalid,
		"foofoo": Invalid,
		"":       Invalid,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		assert.Equal(t, BigQueryTypeToKind(bqCol), expectedKind, fmt.Sprintf("bqCol: %s did not match", bqCol))
	}
}
