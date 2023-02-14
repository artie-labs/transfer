package typing

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/ext"
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
		"datetime":  NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		"timestamp": NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		"time":      NewKindDetailsFromTemplate(ETime, ext.TimeKindType),
		"date":      NewKindDetailsFromTemplate(ETime, ext.DateKindType),
		//Invalid
		"foo":    Invalid,
		"foofoo": Invalid,
		"":       Invalid,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		assert.Equal(t, BigQueryTypeToKind(bqCol), expectedKind, fmt.Sprintf("bqCol: %s did not match", bqCol))
	}
}

func TestBigQueryTypeNoDataLoss(t *testing.T) {
	kindDetails := []KindDetails{
		NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.TimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.DateKindType),
		String,
		Boolean,
		Struct,
	}

	for _, kindDetail := range kindDetails {
		assert.Equal(t, kindDetail, BigQueryTypeToKind(KindToBigQuery(kindDetail)))
	}
}
